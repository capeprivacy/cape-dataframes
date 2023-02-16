import abc


class AbstractTransformation(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def dtype(self):
        pass

    @abc.abstractmethod
    def __call__(self, x):
        pass


class Transformation(AbstractTransformation):
    def __init__(self, dtype):
        self._dtype = dtype

    @property
    def dtype(self):
        return self._dtype
