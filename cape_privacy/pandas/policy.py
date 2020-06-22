from typing import List

import pandas as pd

from cape_privacy.pandas import registry
from cape_privacy.policy import data
from cape_privacy.policy import policy as policy_commons


def do_transformations(policy: data.Policy, rule: data.Rule, df: pd.DataFrame):
    """Applies a specific rule's transformations to a pandas dataframe.

    For each transform, lookup the required transform class and then apply it
    to the correct column in that dataframe.

    Args:
        policy: The top level policy.
        rule: The specific rule to apply.
        df: A pandas dataframe.

    Returns:
        The resulting transformed pandas dataframe.
    """

    for transform in rule.transformations:
        do_transform = policy_commons.get_transformation(
            policy, transform, registry, return_spark=False
        )
        if do_transform.type_signature == "df->df":
            df = do_transform(df)
        else:
            df[transform.field] = do_transform(df[transform.field])

    return df


def apply_policies(policies: List[data.Policy], df: pd.DataFrame):
    """Applies a list of policies to a pandas dataframe.

    Args:
        policies: List of policy objects to apply
        df: A pandas dataframe

    Returns:
        The resulting transformed pandas dataframe.
    """
    for policy in policies:
        for rule in policy.rules:
            df = do_transformations(policy, rule, df)

    return df
