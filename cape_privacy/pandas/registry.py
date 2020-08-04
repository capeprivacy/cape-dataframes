from typing import Callable
from typing import Dict

from cape_privacy.pandas.transformations import ColumnRedact
from cape_privacy.pandas.transformations import DatePerturbation
from cape_privacy.pandas.transformations import DateTruncation
from cape_privacy.pandas.transformations import NumericPerturbation
from cape_privacy.pandas.transformations import NumericRounding
from cape_privacy.pandas.transformations import ReversibleTokenizer
from cape_privacy.pandas.transformations import RowRedact
from cape_privacy.pandas.transformations import Tokenizer
from cape_privacy.pandas.transformations import TokenReverser

TransformationCtor = Callable

_registry: Dict[str, TransformationCtor] = {}


def get(transformation: str) -> TransformationCtor:
    """Returns the constructor for the given key.

    Arguments:
        transformation: The key of transformation to retrieve.
    """
    return _registry.get(transformation, None)


def register(label: str, ctor: TransformationCtor):
    """Registers a new transformation constructor under the label provided.

    Arguments:
        label: The label that will be used as the key in the registry
        ctor: The transformation constructor
    """
    _registry[label] = ctor


register(DatePerturbation.identifier, DatePerturbation)
register(NumericPerturbation.identifier, NumericPerturbation)
register(NumericRounding.identifier, NumericRounding)
register(Tokenizer.identifier, Tokenizer)
register(DateTruncation.identifier, DateTruncation)
register(ColumnRedact.identifier, ColumnRedact)
register(RowRedact.identifier, RowRedact)
register(TokenReverser.identifier, TokenReverser)
register(ReversibleTokenizer.identifier, ReversibleTokenizer)
