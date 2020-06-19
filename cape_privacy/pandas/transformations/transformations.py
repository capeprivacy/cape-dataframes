from typing import Any
from typing import Callable
from typing import Dict

from cape_privacy.pandas.transformations.column_redact import ColumnRedact
from cape_privacy.pandas.transformations.row_redact import RowRedact

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


register("redact_column", ColumnRedact)
register("redact_row", RowRedact)
