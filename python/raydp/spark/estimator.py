from abc import ABC, abstractmethod
from typing import Any, NoReturn

from raydp.spark.utils import df_type_check


class EstimatorInterface(ABC):
    """
    A scikit-learn like API.
    """

    @abstractmethod
    def fit(self, df, **kwargs) -> NoReturn:
        """
        Fit the model on the df. The df should be ether spark DataFrame or koalas DataFrame.
        """
        df_type_check(df)

    @abstractmethod
    def evaluate(self, df: Any, **kwargs) -> NoReturn:
        """
        Evaluate on the trained model. The df should be ether spark DataFrame or koalas
        DataFrame. This should be called after call fit.
        """
        df_type_check(df)

    @abstractmethod
    def get_model(self) -> Any:
        """
        Get the trained model
        :return the model
        """
        pass

    @abstractmethod
    def save(self, file_path) -> NoReturn:
        """
        Save the trained model to the given file path
        :param file_path: the file path
        """
        pass

    @abstractmethod
    def restore(self, file_path) -> NoReturn:
        """
        Restore the model
        :param file_path: the model saved file path
        """
        pass

    @abstractmethod
    def shutdown(self) -> NoReturn:
        """
        Shutdown the estimator
        """
        pass
