from abc import ABC, abstractmethod

from typing import Any, NoReturn

from raydp.spark.utils import df_type_check


class EstimatorInterface(ABC):
    """
    A scikit-learn like API.
    """

    @abstractmethod
    def fit(self, df: Any) -> NoReturn:
        """
        Fit the model on the df. The df should be ether spark DataFrame or koalas DataFrame.
        """
        df_type_check(df)

    @abstractmethod
    def evaluate(self, df: Any) -> NoReturn:
        """
        Evaluate on the trained model. The df should be ether spark DataFrame or koalas
        DataFrame. This should be called after call fit.
        """
        df_type_check(df)


