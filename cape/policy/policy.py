import re
from typing import Any
from typing import Dict

import yaml

from cape.transformations import get

from .exceptions import NamedTransformNotFound
from .exceptions import TransformNotFound
from .utils import yaml_args_to_kwargs

TYPE_INDEX = 1
COLLECTION_INDEX = 2
ENTITY_INDEX = 3


def get_transformations(policy: Dict[Any, Any], rule: Dict[Any, Any]):
    transforms = []
    for transform in rule["transformations"]:
        args = None
        if "args" in transform:
            args = yaml_args_to_kwargs(transform["args"])

        if "function" in transform:
            try:
                initTransform = get(transform["function"])(transform["field"], **args)
            except KeyError:
                function = transform["function"]
                raise TransformNotFound(f"Could not find builtin transform {function}")
        elif "named" in transform:
            try:
                initTransform = load_named_transform(
                    policy, transform["named"], transform["field"]
                )
            except KeyError:
                named = transform["named"]
                raise NamedTransformNotFound(f"Could not find named transform {named}")
        else:
            field = transform["field"]
            raise KeyError(
                f"Expected function or named for transform with field {field}"
            )

        transforms.append(initTransform)

    return transforms


def apply_policies(
    policies: [Dict[Any, Any]], entity: str, df,
):
    for policy in policies:
        for rule in policy["spec"]["rules"]:
            res = re.match(r"^(.*):(.*)\.(.*)$", rule["target"])
            if res is None:
                continue

            if res.group(ENTITY_INDEX) == entity and "transformations" in rule:
                transforms = get_transformations(policy, rule)

                for transform in transforms:
                    df[transform.field] = transform.transform(df[transform.field])

    return df


def parse_policy(file: str):
    with open(file) as f:
        data = f.read()

    policy = yaml.load(data, Loader=yaml.FullLoader)

    return policy


def load_named_transform(policy: Dict[Any, Any], transformLabel: str, field: str):
    found = False

    named_transforms = policy["transformations"]
    for transform in named_transforms:
        if transformLabel == transform["name"]:
            args = None
            if "args" in transform:
                args = yaml_args_to_kwargs(transform["args"])

            initTransform = get(transform["type"])(field, **args)
            found = True
            break

    if not found:
        raise NamedTransformNotFound(
            f"Could not find transform {transformLabel} in transformations block"
        )

    return initTransform
