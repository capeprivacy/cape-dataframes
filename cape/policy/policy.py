import re
from typing import Any
from typing import Dict

import requests
import validators
import yaml

from cape.transformations import get

from .data import Policy
from .data import Rule
from .exceptions import NamedTransformNotFound
from .exceptions import TransformNotFound

TYPE_INDEX = 1
COLLECTION_INDEX = 2
ENTITY_INDEX = 3


def get_transformations(policy: Policy, rule: Rule):
    transforms = []
    for transform in rule.transformations:
        if transform.function != "":
            try:
                initTransform = get(transform.function)(
                    transform.field, **transform.args
                )
            except KeyError:
                function = transform.function
                raise TransformNotFound(f"Could not find builtin transform {function}")
        elif transform.named != "":
            try:
                initTransform = load_named_transform(
                    policy, transform.named, transform.field
                )
            except KeyError:
                raise NamedTransformNotFound(
                    f"Could not find named transform {transform.named}"
                )
        else:
            raise KeyError(
                f"Expected function or named for transform with field {transform.field}"
            )

        transforms.append(initTransform)

    return transforms


def apply_policies(
    policies: [Policy], entity: str, df,
):
    for policy in policies:
        for rule in policy.spec.rules:
            res = re.match(r"^(.*):(.*)\.(.*)$", rule.target)
            if res is None:
                continue

            if res.group(ENTITY_INDEX) == entity:
                transforms = get_transformations(policy, rule)

                for transform in transforms:
                    df[transform.field] = transform(df[transform.field])

    return df


def parse_policy(p: str):
    data: str

    if validators.url(p):
        data = requests.get(p).text
    else:
        with open(p) as f:
            data = f.read()

    policy = yaml.load(data, Loader=yaml.FullLoader)
    return Policy(**policy)


def load_named_transform(policy: Dict[Any, Any], transformLabel: str, field: str):
    found = False

    named_transforms = policy.transformations
    for transform in named_transforms:
        if transformLabel == transform.name:
            initTransform = get(transform.type)(field, **transform.args)
            found = True
            break

    if not found:
        raise NamedTransformNotFound(
            f"Could not find transform {transformLabel} in transformations block"
        )

    return initTransform
