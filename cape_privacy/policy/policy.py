"""Utils for parsing policy and applying them.

The module reads in policy as yaml and then through apply_policies
and applies them to pandas dataframes.

    Typical usage example:

    Example policy yaml:

    label: test_policy
    spec:
      version: 1
      label: test_policy
      rules:
        # The last transactions should match the entity
        # passed below.
        - target: records:transactions.transactions
          action: read
          effect: allow
          transformations:
            # Tells the policy runner to apply the transformation
            # plusN with the specified args.
            - field: value
              function: plusN
              args:
                n:
                  value: 1
            # Tells the policy runner to apply another plusN
            # transformation.
            - field: value
              function: plusN
              args:
                n:
                  value: 2

    Applying policy:

    policy = parse_policy("policy.yaml")

    entity = "transactions"

    df = pd.DataFrame(np.ones(5,), columns=["value"])

    df = apply_policies([policy], entity, df)
"""

import re
from typing import Any
from typing import Dict

import pandas as pd
import requests
import validators
import yaml

from cape_privacy.pandas.transformations import get

from .data import Policy
from .data import Rule
from .data import Transform
from .exceptions import NamedTransformNotFound
from .exceptions import TransformNotFound

TYPE_INDEX = 1
COLLECTION_INDEX = 2
ENTITY_INDEX = 3


def get_transformation(policy: Policy, transform: Transform):
    """Looks up the correct transform class.

    If the transform is anonymous (i.e. unnamed) then it looks it up from the
    transform registry. If it is a named transform it used load_named_transform
    to find it.

    Args:
        policy: The top level policy.
        transform: The specific transform to be applied.

    Returns:
        The initialize transform object.

    Raises:
        TransformNotFound: The builtin transform cannot be found.
        NamedTransformNotFound: The named transform cannot be found on the
        top level policy object.
        KeyError: If neither a function or named transform exists on the transform arg.
    """
    if transform.function is not None:
        try:
            initTransform = get(transform.function)(**transform.args)
        except KeyError:
            raise TransformNotFound(
                f"Could not find builtin transform {transform.function}"
            )
    elif transform.named is not None:
        try:
            initTransform = load_named_transform(policy, transform.named)
        except KeyError:
            raise NamedTransformNotFound(
                f"Could not find named transform {transform.named}"
            )
    else:
        raise KeyError(
            f"Expected function or named for transform with field {transform.field}"
        )

    return initTransform


def do_redaction(rule: Rule, df: pd.DataFrame):
    """Handles redacting columns and rows.

    If redact is set in a rule then it redacts all of the
    specified columns.

    If where is set in a rule then the condition is passed into
    redact_row transformation and redacts all columns where the
    condition is true.

    Arguments:
        rule: The rule to process.
        df: The dataframe to redact from.

    Returns:
        The redacted or un-redacted dataframe depending what is in the
        rule.
    """
    if rule.redact is not None:
        redact = get("redact_column")(rule.redact)
        df = redact(df)

    if rule.where is not None:
        redact = get("redact_row")(rule.where)
        df = redact(df)

    return df


def do_transformations(policy: Policy, rule: Rule, df: pd.DataFrame):
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
    df = do_redaction(rule, df)

    for transform in rule.transformations:
        initTransform = get_transformation(policy, transform)

        df[transform.field] = initTransform(df[transform.field])

    return df


def apply_policies(
    policies: [Policy], entity: str, df,
):
    """Applies a list of policies to a pandas dataframe.

    For each rule in each policy, if there is a target matching the
    entity label passed in then each transform in that rule is applied to
    the dataframe. If there is a where clause in the rule then each column
    that matches the where is redacted from the final dataframe.

    Args:
        policies: List of policy objects to apply
        entity: The entity label of the dataframe
        df: A pandas dataframe

    Returns:
        The resulting transformed pandas dataframe.
    """
    for policy in policies:
        for rule in policy.spec.rules:
            res = re.match(r"^(.*):(.*)\.(.*)$", rule.target)
            if res is None:
                continue

            if res.group(ENTITY_INDEX) == entity:
                df = do_transformations(policy, rule, df)

    return df


def parse_policy(p: str):
    """Parses a policy yaml file.

    The passed in string can either be a path to a local file or
    a URL pointing to a file. If it is a URL then requests attempts to download it.

    Args:
        p: a path string or a URL string

    Returns:
        The Policy object initialized by the yaml.
    """
    data: str

    if validators.url(p):
        data = requests.get(p).text
    else:
        with open(p) as f:
            data = f.read()

    policy = yaml.load(data, Loader=yaml.FullLoader)
    return Policy(**policy)


def load_named_transform(policy: Dict[Any, Any], transformLabel: str):
    """Attempts to load a named transform from the top level policy.

    Looks at the top level policy object for the named transform given as transformLabel
    and initializes it from the args pulled from the policy object.

    Args:
        policy: Top level policy object.
        transformLabel: The name of the named transform.
        field: The field to which the transform will be applied.

    Returns:
        The initialized transform object.

    Raises:
        NamedTransformNotFound: The named transform cannot be
        found in the top level policy object.
    """
    found = False

    named_transforms = policy.transformations
    for transform in named_transforms:
        if transformLabel == transform.name:
            initTransform = get(transform.type)(**transform.args)
            found = True
            break

    if not found:
        raise NamedTransformNotFound(
            f"Could not find transform {transformLabel} in transformations block"
        )

    return initTransform
