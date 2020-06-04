import pickle
import time
from collections import defaultdict
from typing import Dict, List, NoReturn, Optional, Tuple

import pandas as pd
import pyarrow.plasma as plasma
import ray
import ray.cloudpickle as rpickle
import ray.worker


@ray.remote(num_cpus=0)
def save(value: List[ray.ObjectID]) -> List[ray.ObjectID]:
    value = value[0]
    tmp = ray.get(value)
    result = ray.put(tmp)
    return [result]


@ray.remote(num_cpus=0)
class BlockHolder:
    """
    A block holder alive on each node.
    """
    def __init__(self, node_label: str, concurrent_save=10, save_interval=5):
        # hold the ObjectID to increase the ray inner reference counter
        self._data: Dict[int, ray.ObjectID] = {}
        self._data_reference_counter: Dict[int, int] = {}
        self._fetch_index: int = 0

        self._save_fn = save.options(resources={node_label: 0.01})
        self._concurrent_save = concurrent_save
        self._save_interval = save_interval
        self._save_pool = []
        self._save_pool_fetch_index = []
        self._last_save_time = time.time()

    def _update_remote_save(self):
        objs = ray.get(self._save_pool)
        for index, obj in zip(self._save_pool_fetch_index, objs):
            self._data[index] = obj[0]
        self._save_pool = []
        self._save_pool_fetch_index = []
        self._last_save_time = time.time()

    def _pin_object(self, object_id: ray.ObjectID, fetch_index: int):
        """
        TODO: add support object owner transfer in ray
        Pin the object in block holder, this should be fixed when we support transfer object
        owner in ray.
        :param object_id: the original object id
        :param fetch_index: the fetch index that can fetch the given data
        """
        cur_time = time.time()
        if ((len(self._save_pool) > 0 and len(self._save_pool) == self._concurrent_save)
                or (self._last_save_time - cur_time) > self._save_interval):
            self._update_remote_save()

        remote_id = self._save_fn.remote([object_id])
        self._save_pool.append(remote_id)
        self._save_pool_fetch_index.append(fetch_index)

    def register_object_id(self, object_ids: List[ray.ObjectID]) -> List[int]:
        """
        Register a list of object id to hold.
        :param object_ids: list of ray object id
        :return: fetch indexes that can fetch the given value with get_object
        """
        results = []
        for object_id in object_ids:
            fetch_index = self._fetch_index
            self._fetch_index += 1
            self._pin_object(object_id, fetch_index)
            self._data_reference_counter[fetch_index] = 0
            results.append(fetch_index)

        if len(results) == 1:
            results = results[0]

        return results

    def get_object(self, fetch_indexes: List[int]) -> List[Optional[ray.ObjectID]]:
        """
        Get registered ObjectId. This will increase the ObjectId reference counter.
        :param fetch_indexes: the fetch index which used to look the mapping ObjectId
        :return: ObjectId or None if it is not found
        """
        results = []
        for index in fetch_indexes:
            if index in self._save_pool_fetch_index:
                self._update_remote_save()

            if index not in self._data:
                results.append(None)
            else:
                # increase the reference counter
                self._data_reference_counter[index] =\
                    self._data_reference_counter[index] + 1
                results.append(self._data[index])

        return results

    def remove_object_id(self, fetch_index: int, destroy: bool = False) -> NoReturn:
        """
        Remove the reference for the ObjectId
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        :param destroy: whether destroy the ObjectId when the reference counter is zero
        """
        if fetch_index in self._save_pool_fetch_index:
            self._update_remote_save()

        if fetch_index in self._data:
            assert self._data_reference_counter[fetch_index] > 0
            self._data_reference_counter[fetch_index] =\
                self._data_reference_counter[fetch_index] - 1

            if destroy and self._data_reference_counter[fetch_index] == 0:
                self.destroy_object_id(fetch_index)

    def remove_object_ids(self, fetch_indexes: List[int], destroy: bool = False) -> NoReturn:
        for fetch_index in fetch_indexes:
            self.remove_object_id(fetch_index, destroy)

    def destroy_object_id(self, fetch_index) -> NoReturn:
        """
        Destroy the ObjectId directly.
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        """
        if fetch_index in self._save_pool_fetch_index:
            self._update_remote_save()

        if fetch_index in self._data:
            del self._data[fetch_index]
            del self._data_in_bytes[fetch_index]
            del self._data_reference_counter[fetch_index]

    def destroy_object_ids(self, fetch_indexes: List[int]) -> NoReturn:
        for fetch_index in fetch_indexes:
            self.destroy_object_id(fetch_index)

    def stop(self) -> None:
        """
        Clean all data.
        """
        self._update_remote_save()
        self.destroy_object_ids(list(self._data.keys()))


class BlockHolderActorHandlerWrapper:
    """
    A BlockHolder actor handler wrapper to support deserialize the the handler after
    ray initialized.
    """
    def __init__(self, block_holder: BlockHolder):
        """
        :param block_holder: the BlockHolder actor handler
        """
        self._block_holder = block_holder

    def __getattr__(self, item):
        if not self._block_holder:
            self._lazy_deserialize()

        return getattr(self._block_holder, item)

    @classmethod
    def _custom_deserialize(cls, serialized_data):
        instance = cls(None)
        instance._serialized_data = serialized_data
        return instance

    def _lazy_deserialize(self):
        """
        This should be called after ray has been initialized.
        """
        assert ray.is_initialized()
        self._block_holder = rpickle.loads(self._serialized_data)

    def __reduce__(self):
        serialized_data = rpickle.dumps(self._block_holder)
        return BlockHolderActorHandlerWrapper._custom_deserialize, (serialized_data,)


class BlockSet:
    """
    A list of fetch index, and each fetch index is wrapper into a Block.

    The workflow of this class:
       1. block_set = BlockSet()  # create instance
       2. block_set.append() or hold_df.append_batch() # add fetch_index data
       3. block_set.resolve()  # Resolve the BlockSet and can't add data again after resolve.
       4. block_set[0].get() # get the underlying data which should be a pandas.DataFrame
    """
    def __init__(self,
                 fetch_indexes: List[Tuple[str, int]],
                 block_sizes: List[int],
                 block_holder_mapping: Dict[str, BlockHolderActorHandlerWrapper]):
        assert len(fetch_indexes) == len(block_sizes),\
            "The length of fetch_indexes and block_sizes should be equalled"
        self._fetch_indexes: List[Tuple[str, int]] = []
        self._block_sizes = block_sizes
        self._total_size = sum(self._block_sizes)
        self._block_holder_mapping = block_holder_mapping

        self._resolved = False
        self._resolved_block: Dict[int, ray.ObjectID] = {}

        self._plasma_store_socket_name = None
        self._plasma_client = None

        self.append_batch(fetch_indexes)

    @property
    def total_size(self) -> int:
        return self._total_size

    @property
    def block_sizes(self) -> List[int]:
        return self._block_sizes

    @property
    def resolved_indices(self):
        assert self._resolved, "The blockset has not been resolved"
        return self.resolved_indices

    def append(self, node_label: str, fetch_index) -> NoReturn:
        assert not self._resolved, "Can not append value after resolved"
        self._fetch_indexes.append((node_label, fetch_index))

    def append_batch(self, fetch_indexes: List[Tuple[str, int]]) -> NoReturn:
        assert not self._resolved, "Can not append value after resolved"
        self._fetch_indexes.extend(fetch_indexes)

    def _fetch_objects_without_deserialization(self, object_ids, timeout=None) -> NoReturn:
        """
        This is just fetch object from remote object store to local and without deserialization.
        :param object_ids: Object ID of the object to get or a list of object IDs to
            get.
        :param timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.
        """
        is_individual_id = isinstance(object_ids, ray.ObjectID)
        if is_individual_id:
            object_ids = [object_ids]

        if not isinstance(object_ids, list):
            raise ValueError("'object_ids' must either be an object ID "
                             "or a list of object IDs.")

        worker = ray.worker.global_worker
        worker.check_connected()
        timeout_ms = int(timeout * 1000) if timeout else -1
        worker.core_worker.get_objects(object_ids, worker.current_task_id, timeout_ms)

    def resolve(self, indices: Optional[List[int]]) -> NoReturn:
        """
        Resolve the given indices blocks in this block set.
        :param indices: the block indices
        """
        if self._resolved:
            # TODO: should we support resolve with different indices?
            resolved_indices = sorted(self._resolved_block.keys())
            assert resolved_indices == sorted(indices)
            return

        if indices is None:
            indices = range(len(self._blocks))

        grouped = defaultdict(lambda: [])
        label_to_indexes = defaultdict(lambda: [])
        succeed = {}
        # group indices by block holder
        for i in indices:
            label, index = self._fetch_indexes[i]
            grouped[label].append(index)
            label_to_indexes[label].append(i)

        for label in grouped:
            holder = self._block_holder_mapping.get(label, None)
            assert holder, f"Can't find the DataHolder for the label: {label}"
            object_ids = ray.get(holder.get_object.remote(grouped[label]))
            try:
                # just trigger object transfer without object deserialization.
                self._fetch_objects_without_deserialization(object_ids)
            except Exception as exp:
                # deserialize or ray.get failed, we should decrease the reference
                for resolved_label in succeed:
                    ray.get(holder.remove_object_ids.remote(grouped[resolved_label]))
                raise exp

            succeed[label] = object_ids

        for label in succeed:
            data = succeed[label]
            indexes = label_to_indexes[label]
            for i, d in zip(indexes, data):
                self._resolved_block[i] = d

        self._resolved = True

    def set_resolved_block(self, resolved_block: Dict[int, ray.ObjectID]) -> NoReturn:
        if len(resolved_block) == 0:
            return

        expected_indexes = sorted(resolved_block.keys())
        if self._resolved:
            # TODO: should we support resolve with different indices?
            resolved_indices = sorted(self._resolved_block.keys())
            assert resolved_indices == expected_indexes
            return

        fetch_indexes_len = len(self._fetch_indexes)
        for i in expected_indexes:
            if i >= fetch_indexes_len:
                raise ValueError(
                    f"The index: {i} is out of range, the blockset size is {fetch_indexes_len}")

        self._resolved = True
        self._resolved_block = resolved_block

    def set_plasma_store_socket_name(self, plasma_store_socket_name: Optional[str]):
        if self._plasma_store_socket_name is None:
            self._plasma_store_socket_name = plasma_store_socket_name

    def clean(self, destroy: bool = False) -> NoReturn:
        if not self._resolved:
            return
        grouped = defaultdict(lambda: [])
        for i in self._resolved_block:
            label, index = self._fetch_indexes[i]
            grouped[label].append(index)

        for label in grouped:
            holder = self._block_holder_mapping[label]
            if holder:
                ray.get(holder.remove_object_ids.remote(grouped[label], destroy))

        self._fetch_indexes: List[Tuple[str, int]] = []
        self._resolved = False
        self._resolved_block = {}
        self._block_holder_mapping = None

    def get_object_id(self, item) -> Optional[ray.ObjectID]:
        assert self._resolved, "You should resolve the blockset before get item"
        return self._resolved_block.get(item, None)

    def _connect_to_plasma(self):
        assert self._plasma_store_socket_name is not None, "You should set the plasma_store_socket_name"
        self._plasma_client = plasma.connect(self._plasma_store_socket_name)

    def __getitem__(self, item) -> pd.DataFrame:
        assert self._resolved, "You should resolve the blockset before get item"
        if self._plasma_client is None:
            self._connect_to_plasma()
        object_id = self._resolved_block.get(item, None)
        assert object_id is not None, f"The {item} block has not been resolved"
        plasma_object_id = plasma.ObjectID(object_id.binary())
        # this should be really faster becuase of zero copy
        data = self._plasma_client.get_buffers([plasma_object_id])[0]
        return pickle.loads(data)

    def __len__(self):
        """This return the block sizes in this block set"""
        return len(self._block_sizes)

    @classmethod
    def _custom_deserialize(cls,
                            fetch_indexes: List[Tuple[str, int]],
                            block_sizes: List[int],
                            block_holder_mapping: Dict[str, BlockHolderActorHandlerWrapper],
                            resolved_block: Dict[int, ray.ObjectID],
                            plasma_store_socket_name: str):
        instance = cls(fetch_indexes, block_sizes, block_holder_mapping)
        instance.set_resolved_block(resolved_block)
        instance.set_plasma_store_socket_name(plasma_store_socket_name)
        return instance

    def __reduce__(self):
        return (BlockSet._custom_deserialize,
                (self._fetch_indexes, self._block_sizes, self._block_holder_mapping,
                 self._resolved_block, self._plasma_store_socket_name))

    def __del__(self):
        if ray.is_initialized():
            self.clean()
