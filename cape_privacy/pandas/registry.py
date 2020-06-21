from typing import Any
from typing import Callable
from typing import Dict

from cape_privacy.pandas.transformations import ColumnRedact
from cape_privacy.pandas.transformations import DatePerturbation
from cape_privacy.pandas.transformations import DateTruncation
from cape_privacy.pandas.transformations import NumericPerturbation
from cape_privacy.pandas.transformations import NumericRounding
from cape_privacy.pandas.transformations import RowRedact
from cape_privacy.pandas.transformations import Tokenizer

TransformationCtor = Callable[[str, Dict[Any, Any]], None]

_registry: Dict[str, TransformationCtor] = {}


def get(transformation: str) -> TransformationCtor:
    """Returns the constructor for the given key.

    Arguments:
        transformation: The key of transformation to retrieve.
    """
    return _registry[transformation]


def register(label: str, ctor: TransformationCtor):
    """Registers a new transformation constructor under the label provided.

    Arguments:
        label: The label that will be used as the key in the registry
        ctor: The transformation constructor
    """
    _registry[label] = ctor


register("date-perturbation", DatePerturbation)
register("numeric-perturbation", NumericPerturbation)
register("numeric-rounding", NumericRounding)
register("tokenizer", Tokenizer)
register("date-truncation", DateTruncation)
register("redact_column", ColumnRedact)
register("redact_row", RowRedact)
