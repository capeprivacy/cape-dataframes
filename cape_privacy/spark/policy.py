from typing import List

from pyspark import sql
from pyspark.sql import functions

from cape_privacy.policy import data
from cape_privacy.policy import policy as policy_commons
from cape_privacy.spark import registry


def _do_transformations(policy: data.Policy, rule: data.Rule, df: sql.DataFrame):
    """Applies a specific rule's transformations to a pandas dataframe.

    For each transform, lookup the required transform class and then apply it
    to the correct column in that dataframe.

    Args:
        policy: The top level policy.
        rule: The specific rule to apply.
        df: A pandas dataframe.

    Returns:
        The transformed dataframe.
    """

    for transform in rule.transformations:
        do_transform = policy_commons.get_transformation(
            policy, transform, registry, return_spark=True
        )
        if do_transform.type_signature == "df->df":
            df = do_transform(df)
        else:
            field_column = functions.col(transform.field)
            df = df.withColumn(transform.field, do_transform(field_column))

    return df


def apply_policies(policies: List[data.Policy], df: sql.DataFrame):
    for policy in policies:
        for rule in policy.rules:
            df = _do_transformations(policy, rule, df)

    return df
