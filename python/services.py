from abc import ABC, abstractmethod

import ray


class MasterService(ABC):

    @abstractmethod
    def start_up(self) -> bool:
        pass

    @abstractmethod
    def get_master_url(self) -> str:
        pass

    def get_host(self) -> str:
        return ray.services.get_node_ip_address()

    @abstractmethod
    def kill(self):
        pass


class WorkerService(ABC):

    @abstractmethod
    def start_up(self) -> bool:
        pass

    def get_host(self) -> str:
        return ray.services.get_node_ip_address()

    @abstractmethod
    def kill(self):
        pass
