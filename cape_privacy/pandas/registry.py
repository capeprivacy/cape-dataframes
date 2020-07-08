from typing import Callable, Dict

from cape_privacy.pandas.transformations import (
    ColumnRedact,
    DatePerturbation,
    DateTruncation,
    NumericPerturbation,
    NumericRounding,
    RowRedact,
    Tokenizer,
)

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
