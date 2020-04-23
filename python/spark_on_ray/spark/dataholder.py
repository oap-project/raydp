from collections import defaultdict
import pandas as pd

import ray
import ray.cloudpickle as rpickle

from typing import Any, Dict, List, Optional, Tuple


@ray.remote(num_cpus=0)
class DataHolder:
    """
    An data holder alive on each node.
    """
    def __init__(self):
        # hold the ObjectID to increase the ray inner reference counter
        self._data: Dict[int, ray.ObjectID] = {}
        self._data_in_bytes: Dict[int, bytes] = {}
        self._data_reference_counter: Dict[int, int] = {}
        self._fetch_index: int = 0

    def register_object_id(self, id: bytes) -> int:
        """
        Register one object id to hold.
        :param id: serialized ObjectId
        :return: fetch index that can fetch the serialized ObjectId with get_object
        """
        fetch_index = self._fetch_index
        self._fetch_index += 1
        self._data[fetch_index] = rpickle.loads(id)
        self._data_in_bytes[fetch_index] = id
        self._data_reference_counter[fetch_index] = 0
        return fetch_index

    def register_object_ids(self, ids: List[bytes]) -> List[int]:
        fetch_indexes = []
        for id in ids:
            fetch_indexes.append(self.register_object_id(id))
        return fetch_indexes

    def get_object(self, fetch_index) -> Optional[bytes]:
        """
        Get registered ObjectId. This will increase the ObjectId reference counter.
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        :return: serialized ObjectId or None if it is not found
        """
        if fetch_index not in self._data:
            return None
        # increase the reference counter
        self._data_reference_counter[fetch_index] = self._data_reference_counter[fetch_index] + 1
        return self._data_in_bytes[fetch_index]

    def get_objects(self, fetch_indexes: List[int]) -> List[Optional[bytes]]:
        return [self.get_object(i) for i in fetch_indexes]

    def remote_object_id(self, fetch_index: int, destroy: bool = False) -> None:
        """
        Remove the reference for the ObjectId
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        :param destroy: whether destroy the ObjectId when the reference counter is zero
        """
        if fetch_index in self._data:
            assert self._data_reference_counter[fetch_index] > 0
            self._data_reference_counter[fetch_index] =\
                self._data_reference_counter[fetch_index] - 1

            if destroy and self._data_reference_counter[fetch_index] == 0:
                self.destroy_object_id(fetch_index)

    def remote_object_ids(self, fetch_indexes: List[int], destroy: bool = False) -> None:
        for fetch_index in fetch_indexes:
            self.remote_object_id(fetch_index, destroy)

    def destroy_object_id(self, fetch_index) -> None:
        """
        Destroy the ObjectId directly.
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        """
        if fetch_index in self._data:
            del self._data[fetch_index]
            del self._data_in_bytes[fetch_index]
            del self._data_reference_counter[fetch_index]

    def destroy_object_ids(self, fetch_indexes: List[int]) -> None:
        for fetch_index in fetch_indexes:
            self.destroy_object_id(fetch_index)

    def stop(self) -> None:
        """
        Clean all data.
        """
        self.destroy_object_ids(list(self._data.keys()))


class DataHolderActorHandlerWrapper:
    def __init__(self, data_holder: DataHolder):
        self._data_holder = data_holder

    def __getattr__(self, item):
        if not self._data_holder:
            self._lazy_deserialize()

        return getattr(self._data_holder, item)

    @classmethod
    def _custom_deserialize(cls, serialized_data):
        obj = cls(None)
        obj._serialized_data = serialized_data
        return obj

    def _lazy_deserialize(self):
        assert ray.is_initialized()
        self._data_holder = rpickle.loads(self._serialized_data)

    def __reduce__(self):
        serialized_data = rpickle.dumps(self._data_holder)
        return DataHolderActorHandlerWrapper._custom_deserialize, (serialized_data,)


class ObjectIdItem:
    def __init__(self, fetch_index: int):
        self._data_holder = None
        self._node_label = None
        self._fetch_index = fetch_index
        self._data = None
        self._object_id = None
        self._is_valid = True

    def _set_data(self, data: pd.DataFrame) -> None:
        """
        Set the ObjectId and Data directly.

        Either this method or _set_data_holder should be called before any other operations.

        This method should be called by ObjectIdList in batch mode. The data should also be
        freed by ObjectIdList in batch mode.

        :param data: the pandas.DataFrame that the ObjectId point to
        """
        self._data = data

    def _set_data_holder(self,
                         node_label: str,
                         data_holder: DataHolderActorHandlerWrapper) -> None:
        """
        Set node_label and the DataHolder

        Either this method or _set_data should be called before any other operations.

        This is a lazy loading method. The actual data will only be loaded when get or
        object_id method called.

        :param node_label the fetch_index located in node_label
        :param data_holder the actor handler of the DataHolder which used to get/free data.
        """
        self._node_label = node_label
        self._data_holder = data_holder

    def _fetch(self) -> None:
        """
        Fetch data with data_holder. This should be only called when data_holder has set and
        should be called once.
        """
        # fetch ObjectId bytes
        assert self._data_holder, "Actor handler of DataHolder should be set"
        obj_bytes = ray.get(self._data_holder.get_object.remote(self._fetch_index))
        if not obj_bytes:
            raise Exception(f"ObjectId(locate in: {self._node_label}: "
                            f"{self._fetch_index}) has been freed.")
        self._data = ray.get(self._object_id)

    def get(self) -> pd.DataFrame:
        assert self._is_valid
        if not self._data:
            self._fetch()
        return self._data

    def free(self, destroy: bool) -> None:
        """
        Whether destroy the object when there isn't any other references.
        :param destroy:
        :return:
        """
        if self._is_valid:
            if self._data_holder:
                # only need to decrease the reference when we have data_holder.
                ray.get(self._data_holder.remote_object_id.remote(self._fetch_index, destroy))
                del self._data_holder
                self._data_holder = None
                self._node_label = None

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
        return self.__class__, (self._fetch_index,)

    def __del__(self):
        if ray.is_initialized():
            self.free()


class ObjectIdList:
    """
    A list of fetch index, and each fetch index is wrapper into a ObjectIdItem.

    The workflow of this class:
       1. object_id_list = ObjectIdList()  # create instance
       2. object_id_list.append() or object_id_list.append_batch() # add fetch_index data
       3. object_id_list.resolve()  # Resolve the ObjectIdItem and can't add data again after resolve.
       4. object_id_list[0].get() # get the underlying data which should be a pandas.DataFrame
    """
    def __init__(self, fetch_indexes: List[Tuple[str, int]]):
        self._fetch_indexes: List[Tuple[str, int]] = []
        self._data: List[ObjectIdItem] = []
        self._batch_mode = False
        self._resolved = False
        self.append_batch(fetch_indexes)
        self._data_holder_mapping = None

    def append(self, node_label: str, fetch_index) -> None:
        assert not self._resolved
        item = ObjectIdItem(fetch_index)
        self._fetch_indexes.append((node_label, fetch_index))
        self._data.append(item)

    def append_batch(self, fetch_indexes: List[Tuple[str, int]]) -> None:
        assert not self._resolved
        [self.append(label, index) for label, index in fetch_indexes]

    def resolve(self,
                data_holder_mapping: Dict[str, DataHolderActorHandlerWrapper],
                batch: bool) -> None:
        if self._resolved:
            return

        if not batch:
            for i in range(len(self._data)):
                label = self._fetch_indexes[i][0]
                self._data[i]._set_data_holder(label, data_holder_mapping[label])
        else:
            self._data_holder_mapping = data_holder_mapping
            grouped = defaultdict(lambda: [])
            location_mapping = defaultdict(lambda: [])
            resolved = {}
            for i, (label, index) in enumerate(self._fetch_indexes):
                grouped[label].append(index)
                location_mapping[label].append(i)

            for label in grouped:
                holder = data_holder_mapping.get(label, None)
                assert holder
                object_id_bytes = ray.get(holder.get_objects.remote(grouped[label]))
                try:
                    object_ids = [rpickle.loads(obj) for obj in object_id_bytes]
                    objs = ray.get(object_ids)
                except Exception as exp:
                    # deserialize or ray.get failed, we should decrease the reference
                    for resolved_label in resolved:
                        ray.get(holder.remote_object_ids.remote(grouped[resolved_label]))
                    raise exp

                resolved[label] = objs

            for label in resolved:
                datas = resolved[label]
                locations = location_mapping[label]
                [self._data[i]._set_data(data) for i, data in zip(locations, datas)]

            self._batch_mode = True

        self._resolved = True

    def clean(self, destroy: bool = False):
        if not self._resolved:
            return
        if self._batch_mode:
            grouped = defaultdict(lambda: [])
            for label, index in self._fetch_indexes:
                grouped[label].append(index)

            for label in grouped:
                holder = self._data_holder_mapping[label]
                if holder:
                    ray.get(holder.remote_object_ids.remote(grouped[label], destroy))
        else:
            [item.free(destroy) for item in self._data]

        self._fetch_indexes: List[Tuple[str, int]] = []
        self._data: List[ObjectIdItem] = []
        self._batch_mode = False
        self._resolved = False
        self._data_holder_mapping = None

    def __getitem__(self, item) -> ObjectIdItem:
        assert self._resolved
        return self._data[item]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        assert self._resolved
        return self._data.__iter__()

    def __reduce__(self):
        ObjectIdList.__class__, (self._fetch_indexes,)

    def __del__(self):
        if ray.is_initialized():
            self.clean()
