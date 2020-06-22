from typing import Any
from typing import Dict


def yaml_args_to_kwargs(args: Dict[Any, Any]) -> Dict[str, Any]:
    return {key: val["value"] for key, val in args.items()}
