from abc import ABC, abstractmethod


class MasterService(ABC):

    @abstractmethod
    def start_up(self) -> str:
        """
        :return: error message, return None if succeeded
        """
        pass

    @abstractmethod
    def get_master_url(self) -> str:
        pass

    @abstractmethod
    def get_host(self) -> str:
        pass

    @abstractmethod
    def stop(self):
        pass


class WorkerService(ABC):

    @abstractmethod
    def start_up(self) -> str:
        """
        :return: error message, return None if succeeded
        """
        pass

    @abstractmethod
    def get_host(self) -> str:
        pass

    @abstractmethod
    def stop(self):
        pass
