"""Utils for parsing policy and applying them.

The module reads in policy as yaml and then through apply_policy
applies them to dataframes.

Example policy yaml:

    label: test_policy
    version: 1
    rules:
    - match:
        name: value
        actions:
        # Tells the policy runner to apply the transformation
        # plusN with the specified arguments.
        - transform:
            type: plusN
            n: 1
        # Tells the policy runner to apply another plusN
        # transformation.
        - transform:
            type: plusN
            n: 2

Applying policy:

    policy = parse_policy("policy.yaml")
    df = pd.DataFrame(np.ones(5,), columns=["value"])
    df = apply_policy(policy, df)
"""

import copy
import types
from typing import Any
from typing import Callable
from typing import Dict
from typing import Union

import pandas as pd
import requests
import validators
import yaml

from cape_privacy import pandas as pandas_lib
from cape_privacy import spark as spark_lib
from cape_privacy.pandas import transformations
from cape_privacy.policy import data
from cape_privacy.policy import exceptions


def apply_policy(policy: data.Policy, df, inplace=False):
    """Applies a Policy to some DataFrame.

    This function is responsible for inferring the type of the DataFrame, preparing the
    relevant Spark or Pandas Transformations, and applying them to produce a transformed
    DataFrame that conforms to the Policy.

    Args:
        policy: The `Policy` object that the transformed DataFrame will conform to, e.g.
            as returned by `cape_privacy.parse_policy`.
        df: The DataFrame object to transform according to `policies`. Must be of type
            pandas.DataFrame or pyspark.sql.DataFrame.
        inplace: Whether to mutate the `df` or produce a new one. This argument is only
            relevant for Pandas DataFrames, as Spark DataFrames do not support mutation.

    Raises:
        ValueError: If df is a Spark DataFrame and inplace=True, or if df is something
            other than a Pandas or Spark DataFrame.
        DependencyError: If Spark is not configured correctly in the Python environment.
        TransformNotFound, NamedTransformNotFound: If the Policy contains a reference to
            a Transformation or NamedTransformation that is unrecognized in the
            Transformation registry.
    """
    if isinstance(df, pd.DataFrame):
        registry = pandas_lib.registry
        transformer = pandas_lib.transformer
        dtypes = pandas_lib.dtypes
        if not inplace:
            result_df = df.copy()
        else:
            result_df = df
    elif not spark_lib.is_available():
        raise exceptions.DependencyError
    elif isinstance(df, spark_lib.DataFrame):
        if inplace:
            raise ValueError(
                "Spark does not support DataFrame mutation, so inplace=True is invalid."
            )
        registry = spark_lib.registry
        transformer = spark_lib.transformer
        dtypes = spark_lib.dtypes
        result_df = df
    else:
        raise ValueError(f"Expected df to be a DataFrame, found {type(df)}.")
    for rule in policy.rules:
        result_df = _do_transformations(
            policy, rule, result_df, registry, transformer, dtypes
        )
    return result_df


def parse_policy(p: Union[str, Dict[Any, Any]]) -> data.Policy:
    """Parses a policy YAML file.

    The passed in string can either be a path to a local file,
    a URL pointing to a file or a dictionary representing the policy.
    If it is a URL then requests attempts to download it.

    Args:
        p: a path string, a URL string or a dictionary representing the
           policy.

    Returns:
        The Policy object initialized by the YAML.
    """
    if type(p) == str:
        if validators.url(p):
            yaml_data = requests.get(p).text
        else:
            with open(p) as f:
                yaml_data = f.read()

        policy = yaml.load(yaml_data, Loader=yaml.FullLoader)
    else:
        policy = p

    return data.Policy(**policy)


def _maybe_replace_dtype_arg(args, dtypes):
    if "dtype" in args:
        args["dtype"] = getattr(dtypes, args["dtype"])
    return args


def _get_transformation(
    policy: data.Policy, transform: data.Transform, registry: types.ModuleType, dtypes,
):
    """Looks up the correct transform class.

    If the transform is anonymous (i.e. unnamed) then it looks it up from the
    transform registry. If it is a named transform it used load_named_transform
    to find it.

    Args:
        policy: The top level policy.
        transform: The specific transform to be applied.
        registry: The module representing the transformation registry; differs for
            Spark/Pandas.
        dtypes: Passthrough; concrete dtypes to use (spark.dtypes or pandas.dtypes).

    Returns:
        The initialize transform object.

    Raises:
        TransformNotFound: The builtin transform cannot be found.
        NamedTransformNotFound: The named transform cannot be found on the
            top level policy object.
        ValueError: If neither a function or named transform exists on the transform
            arg.
    """
    if transform.type is not None:
        tfm_ctor = registry.get(transform.type)
        if tfm_ctor is None:
            raise exceptions.TransformNotFound(
                f"Could not find builtin transform {transform.type}"
            )
        tfm_args = _maybe_replace_dtype_arg(transform.args, dtypes)
        initTransform = tfm_ctor(**tfm_args)
    elif transform.name is not None:
        initTransform = _load_named_transform(policy, transform.name, registry, dtypes)
    else:
        raise ValueError(
            f"Expected type or name for transform with field {transform.field}"
        )
    return initTransform


def _do_transformations(
    policy: data.Policy,
    rule: data.Rule,
    df,
    registry: types.ModuleType,
    transformer: Callable,
    dtypes,
):
    """Applies a specific rule's transformations to a dataframe.

    For each transform, lookup the required transform class and then apply it
    to the correct column in that dataframe.

    Args:
        policy: The top level policy.
        rule: The specific rule to apply.
        df: A Pandas or Spark dataframe.
        registry: The module representing the transformation registry; differs for
            Spark/Pandas.
        transformer: A function mapping (Transformation, DataFrame, str) to a DataFrame
            that mutates a DataFrame by applying the Transformation to one of its
            columns.
        dtypes: Passthrough; concrete dtypes to use (spark.dtypes or pandas.dtypes).

    Returns:
        The transformed dataframe.
    """

    for transform in rule.transformations:
        do_transform = _get_transformation(policy, transform, registry, dtypes)
        if do_transform.type_signature == "df->df":
            df = do_transform(df)
        else:
            df = transformer(do_transform, df, transform.field)

    return df


def _load_named_transform(
    policy: data.Policy, transformLabel: str, registry: types.ModuleType, dtypes,
):
    """Attempts to load a named transform from the top level policy.

    Looks at the top level policy object for the named transform given as transformLabel
    and initializes it from the args pulled from the policy object.

    Args:
        policy: Top level policy object.
        transformLabel: The name of the named transform.
        registry: The module representing the transformation registry; differs for
            Spark/Pandas.
        dtypes: Passthrough; concrete dtypes to use (spark.dtypes or pandas.dtypes).

    Returns:
        The initialized transform object.

    Raises:
        NamedTransformNotFound: The named transform cannot be
            found in the top level policy object.
        DependencyError: If return_spark is True but PySpark is missing from the current
            environment.
    """
    found = False

    named_transforms = policy.transformations
    for transform in named_transforms:
        if transformLabel == transform.name:
            tfm_ctor = registry.get(transform.type)
            if tfm_ctor is None:
                raise exceptions.NamedTransformNotFound(
                    f"Could not find transform of type {transform.type} in registry"
                )
            tfm_args = _maybe_replace_dtype_arg(transform.args, dtypes)
            initTransform = tfm_ctor(**tfm_args)
            found = True
            break

    if not found:
        raise exceptions.NamedTransformNotFound(
            f"Could not find transform {transformLabel} in transformations block"
        )

    return initTransform


def reverse(policy: data.Policy) -> data.Policy:
    """Turns reversible tokenizations into token reversers

    If any named transformations contain a reversible tokenization transformation
    this helper function turns them into token reverser transformations.

    Args:
        policy: Top level policy object.

    Returns:
        The modified policy.
    """
    new_policy = copy.deepcopy(policy)

    for named in new_policy.transformations:
        if named.type == transformations.ReversibleTokenizer.identifier:
            named.type = transformations.TokenReverser.identifier

    return new_policy
