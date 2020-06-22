import re
from typing import List

from pyspark import sql
from pyspark.sql import functions

from cape_privacy.policy import data
from cape_privacy.policy import policy as policy_commons
from cape_privacy.spark import registry


def _do_redaction(rule: data.Rule, df: sql.DataFrame):
    """Handles redacting columns and rows.

    If redact is set in a rule then it redacts all of the
    specified columns.

    If where is set in a rule then the condition is passed into
    redact_row transformation and redacts  all columns where the
    condition is true.

    Arguments:
        rule: The rule to process.
        df: The dataframe to redact from.

    Returns:
        The redacted or un-redacted dataframe depending what is in the
        rule.
    """
    if rule.redact is not None:
        redact = registry.get("column-redact")(rule.redact)
        df = redact(df)

    if rule.where is not None:
        redact = registry.get("row-redact")(rule.where)
        df = redact(df)

    return df


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

    df = _do_redaction(rule, df)

    for transform in rule.transformations:
        do_transform = policy_commons.get_transformation(
            policy, transform, registry, return_spark=True
        )
        field_column = functions.col(transform.field)
        df = df.withColumn(transform.field, do_transform(field_column))

    return df


def apply_policies(policies: List[data.Policy], entity: str, df: sql.DataFrame):
    for policy in policies:
        for rule in policy.spec.rules:
            res = re.match(r"^(.*):(.*)\.(.*)$", rule.target)
            if res is None:
                continue

            if res.group(policy_commons.ENTITY_INDEX) == entity:
                df = _do_transformations(policy, rule, df)

    return df
