from typing import Callable
from typing import Dict

from cape_privacy.spark.transformations import perturbation
from cape_privacy.spark.transformations import redaction
from cape_privacy.spark.transformations import rounding
from cape_privacy.spark.transformations import tokenizer

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


register(perturbation.DatePerturbation.identifier, perturbation.DatePerturbation)
register(perturbation.NumericPerturbation.identifier, perturbation.NumericPerturbation)
register(rounding.NumericRounding.identifier, rounding.NumericRounding)
register(tokenizer.Tokenizer.identifier, tokenizer.Tokenizer)
register(rounding.DateTruncation.identifier, rounding.DateTruncation)
register(redaction.ColumnRedact.identifier, redaction.ColumnRedact)
register(redaction.RowRedact.identifier, redaction.RowRedact)
register(tokenizer.ReversibleTokenizer.identifier, tokenizer.ReversibleTokenizer)
register(tokenizer.TokenReverser.identifier, tokenizer.TokenReverser)
