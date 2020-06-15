from typing import Any
from typing import Callable
from typing import Dict

from .plus_n import PlusN

TransformationCtor = Callable[[str, Dict[Any, Any]], None]

_registry: Dict[str, TransformationCtor] = {}

_registry["plusN"] = PlusN


def get(transformation: str):
    return _registry[transformation]
