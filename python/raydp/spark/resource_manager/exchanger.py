from abc import ABC, abstractmethod

import pandas as pd


class SharedDataset(ABC):
    """
    A SharedDataset means the Spark DataFrame that have been stored in Ray ObjectStore.
    """

    @abstractmethod
    def __getitem__(self, item) -> pd.DataFrame:
        pass
