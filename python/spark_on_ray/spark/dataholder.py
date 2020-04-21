import ray
import ray.cloudpickle as rpickle

from typing import Any, Dict, List


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

    def register_object_ids(self, ids: List[bytes]):
        fetch_indexes = []
        for id in ids:
            fetch_indexes.append(self.register_object_id(id))
        return fetch_indexes

    def get_object(self, fetch_index) -> Any:
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

    def remote_object_id(self, fetch_index: int, destroy: bool = False):
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

    def remote_object_ids(self, fetch_indexes: List[int], destroy: bool = False):
        for fetch_index in fetch_indexes:
            self.remote_object_id(fetch_index, destroy)

    def destroy_object_id(self, fetch_index):
        """
        Destroy the ObjectId directly.
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        """
        if fetch_index in self._data:
            del self._data[fetch_index]
            del self._data_in_bytes[fetch_index]
            del self._data_reference_counter[fetch_index]

    def destroy_object_ids(self, fetch_indexes: List[int]):
        for fetch_index in fetch_indexes:
            self.destroy_object_id(fetch_index)

    def stop(self):
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


class ObjectIdWrapper:
    def __init__(self, data_holder: DataHolder, fetch_index: int):
        self._data_holder = data_holder
        self._fetch_index = fetch_index
        self._object_id_in_bytes = ray.get(self._data_holder.get_object.remote(self._fetch_index))
        self._object_id = None
        self._is_valid = True

    def get(self) -> Any:
        assert self._is_valid
        if not self._object_id:
            self._object_id = rpickle.loads(self._object_id_in_bytes)
        return ray.get(self._object_id)

    def get_object_id(self):
        assert self._is_valid
        if not self._object_id:
            self._object_id = rpickle.loads(self._object_id_in_bytes)
        return self._object_id

    def free(self, destroy: bool):
        """
        Whether destroy the object when there isn't any other references.
        :param destroy:
        :return:
        """
        if self._is_valid:
            ray.get(self._data_holder.remote_object_id.remote(self._fetch_index, destroy))
            del self._object_id
            del self._data_holder
            self._object_id = None
            self._data_holder = None
            self._is_valid = False
