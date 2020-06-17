import abc


class AbstractTransformation(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def type(self):
        pass

    @abc.abstractmethod
    def __call__(self, x):
        pass


class Transformation(AbstractTransformation):
    def __init__(self, input_type):
        self._type = input_type

    @property
    def type(self):
        return self._type
