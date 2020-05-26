import time
from collections import defaultdict
from typing import Dict, List, NoReturn, Optional, Tuple

import pandas as pd
import ray
import ray.cloudpickle as rpickle


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


class Block:
    def __init__(self, node_label: str, fetch_index: int, expected_len):
        self._node_label = node_label
        self._fetch_index = fetch_index
        self._expeced_len = expected_len

        self._block_holder = None

        self._data = None
        self._is_valid = True

    def _set_data(self, data: pd.DataFrame) -> NoReturn:
        """
        Set the Data directly.

        Either this method or _set_block_holder should be called before any other operations.

        This method should be called by BlockSet in batch mode. The data should also be
        freed by BlockSet in batch mode.

        :param data: the pandas.DataFrame that the ObjectId point to
        """
        assert len(data) == self._expeced_len
        self._data = data

    def _set_block_holder(self,
                          block_holder: BlockHolderActorHandlerWrapper) -> NoReturn:
        """
        Set node_label and the BlockHolder

        Either this method or _set_data should be called before any other operations.

        This is a lazy loading method. The actual data will only be loaded when get or
        object_id method called.

        :param block_holder the actor handler of the BlockHolder which used to get/free data.
        """
        self._block_holder = block_holder

    def _fetch(self) -> NoReturn:
        """
        Fetch data with block_holder. This should be only called when block_holder has set and
        should be called once.
        """
        # fetch ObjectId bytes
        assert self._block_holder, "Actor handler of BlockHolder should be set"
        obj = ray.get(self._block_holder.get_object.remote([self._fetch_index]))
        if obj is None:
            raise Exception(f"ObjectId(locates in: {self._node_label}: "
                            f"{self._fetch_index}) has been freed.")
        self._data = ray.get(obj[0])
        assert len(self._data) == self._expeced_len

    def get(self) -> pd.DataFrame:
        assert self._is_valid
        if self._data is None:
            self._fetch()
        return self._data

    def free(self, destroy: bool) -> NoReturn:
        """
        Free the object reference.
        :param destroy: Whether destroy the object when there isn't any other references.
        """
        if self._is_valid:
            if self._block_holder:
                # only need to decrease the reference when we have block_holder.
                ray.get(self._block_holder.remove_object_id.remote(self._fetch_index, destroy))
                del self._block_holder
                self._block_holder = None

            self._node_label = None
            self._fetch_index = None
            self._data = None
            self._is_valid = False

    def __getitem__(self, item):
        self.get()
        return self._data.__getitem__(item)

    def __len__(self):
        self.get()
        return self._data.__len__()

    def __reduce__(self):
        assert self._is_valid
        return self.__class__, (self._node_label, self._fetch_index, self._expeced_len)

    def __del__(self):
        if ray.is_initialized():
            self.free(True)


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
        self._fetch_indexes: List[Tuple[str, int]] = []
        self._block_sizes = block_sizes
        self._total_size = sum(self._block_sizes)
        self._block_holder_mapping = block_holder_mapping
        self._blocks: List[Block] = []

        self._batch_mode = False

        self._resolved = False
        self._resolved_indices: List[int] = None

        self.append_batch(fetch_indexes, block_sizes)

    @property
    def total_size(self) -> int:
        return self._total_size

    @property
    def block_sizes(self) -> List[int]:
        return self._block_sizes

    @property
    def resolved_indices(self):
        assert self._resolved
        return self.resolved_indices

    def append(self, node_label: str, fetch_index, expected_len) -> NoReturn:
        assert not self._resolved
        block = Block(node_label, fetch_index, expected_len)
        self._fetch_indexes.append((node_label, fetch_index))
        self._blocks.append(block)

    def append_batch(self,
                     fetch_indexes: List[Tuple[str, int]],
                     expected_lens: List[int]) -> NoReturn:
        assert not self._resolved
        for i in range(len(fetch_indexes)):
            self.append(fetch_indexes[i][0], fetch_indexes[i][1], expected_lens[i])

    def resolve(self, indices: List[int], batch: bool = True) -> NoReturn:
        """
        Resolve the given indices blocks in this block set.
        :param indices: the block indices
        :param batch: whether resolve in batch mode
        """
        if self._resolved:
            # TODO: should we support resolve with different indices?
            assert self._resolved_indices == indices
            return

        if indices is None:
            indices = range(len(self._blocks))

        if not batch:
            for i in indices:
                label = self._fetch_indexes[i][0]
                holder = self._block_holder_mapping.get(label, None)
                assert holder, f"Can't find the DataHolder for the label: {label}"
                block = self._blocks[i]
                block._set_block_holder(holder)
        else:
            self._batch_mode = True

            grouped = defaultdict(lambda: [])
            label_to_indexes = defaultdict(lambda: [])
            succeed = {}
            for i in indices:
                label, index = self._fetch_indexes[i]
                grouped[label].append(index)
                label_to_indexes[label].append(i)

            for label in grouped:
                holder = self._block_holder_mapping.get(label, None)
                assert holder, f"Can't find the DataHolder for the label: {label}"
                object_ids = ray.get(holder.get_object.remote(grouped[label]))
                try:
                    data = ray.get(object_ids)
                except Exception as exp:
                    # deserialize or ray.get failed, we should decrease the reference
                    for resolved_label in succeed:
                        ray.get(holder.remove_object_ids.remote(grouped[resolved_label]))
                    raise exp

                succeed[label] = data

            for label in succeed:
                data = succeed[label]
                indexes = label_to_indexes[label]
                [self._blocks[i]._set_data(d) for i, d in zip(indexes, data)]

        self._resolved = True
        self._resolved_indices = indices

    def clean(self, destroy: bool = False) -> NoReturn:
        if not self._resolved:
            return
        if self._batch_mode:
            grouped = defaultdict(lambda: [])
            for i in self._resolved_indices:
                label, index = self._fetch_indexes[i]
                grouped[label].append(index)

            for label in grouped:
                holder = self._block_holder_mapping[label]
                if holder:
                    ray.get(holder.remove_object_ids.remote(grouped[label], destroy))
        else:
            [self._blocks[item].free(destroy) for item in self.resolved_indices]

        self._fetch_indexes: List[Tuple[str, int]] = []
        self._blocks: List[Block] = []
        self._batch_mode = False
        self._resolved = False
        self._resolved_indices = None
        self._block_holder_mapping = None

    def __getitem__(self, item) -> Block:
        assert self._resolved
        return self._blocks[item]

    def __len__(self):
        """This return the block sizes in this block set"""
        return len(self._blocks)

    def __reduce__(self):
        return (self.__class__,
                (self._fetch_indexes, self._block_sizes, self._block_holder_mapping))

    def __del__(self):
        if ray.is_initialized():
            self.clean()
