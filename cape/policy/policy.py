import re
from typing import Any
from typing import Dict

from cape.transformations.transformations import get

TYPE_INDEX = 1
COLLECTION_INDEX = 2
ENTITY_INDEX = 3


def get_transformations(rule: Dict[Any, Any]):
    transforms = []
    for transform in rule["transformations"]:
        args = None
        if "args" in transform:
            args = transform["args"]

        initTransform = get(transform["function"])(transform["field"], args)

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
                transforms = get_transformations(rule)

                for transform in transforms:
                    df[transform.field] = transform.transform(df[transform.field])

    return df
