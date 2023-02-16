import abc

from cape_dataframes.spark import dtypes


class AbstractTransformation(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def dtype(self):
        pass

    @abc.abstractmethod
    def __call__(self, x):
        pass


class Transformation(AbstractTransformation):
    def __init__(self, dtype: dtypes.DType):
        self._dtype = dtype

    @property
    def dtype(self):
        return self._dtype
