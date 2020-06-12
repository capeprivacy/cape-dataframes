from typing import Any
from typing import Callable
from typing import Dict

from .plus_one import PlusOne

TransformationCtor = Callable[[str, Dict[Any, Any]], None]

_registry: Dict[str, TransformationCtor] = {}

_registry["plusOne"] = PlusOne


def get(transformation: str):
    return _registry[transformation]
