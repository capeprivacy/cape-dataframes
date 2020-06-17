from typing import Any
from typing import Dict

from cape_privacy.pandas.transformations import dtypes


def yaml_args_to_kwargs(args: Dict[Any, Any]) -> Dict[str, Any]:
    kwargs = {key: val["value"] for key, val in args.items()}
    if "dtype" in kwargs:
        if isinstance(kwargs["dtype"], str):
            kwargs["dtype"] = getattr(dtypes, kwargs["dtype"])
    return kwargs
