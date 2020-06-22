"""Utils for parsing policy and applying them.

The module reads in policy as yaml and then through apply_policies
and applies them to pandas dataframes.

    Typical usage example:

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

    df = apply_policies([policy], df)
"""

import types

import requests
import validators
import yaml

from cape_privacy import spark
from cape_privacy.pandas import dtypes as pd_dtypes
from cape_privacy.policy import data
from cape_privacy.policy import exceptions

TYPE_INDEX = 1
COLLECTION_INDEX = 2
ENTITY_INDEX = 3


def _maybe_replace_dtype_arg(args, return_spark):
    if return_spark and spark is not None:
        dtypes = getattr(spark, "dtypes")
    else:
        dtypes = pd_dtypes
    if "dtype" in args:
        args["dtype"] = getattr(dtypes, args["dtype"])
    return args


def get_transformation(
    policy: data.Policy,
    transform: data.Transform,
    registry: types.ModuleType,
    return_spark: bool,
):
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
        ValueError: If neither a function or named transform exists on the transform
            arg.
    """
    if transform.type is not None:
        tfm_ctor = registry.get(transform.type)
        if tfm_ctor is None:
            raise exceptions.TransformNotFound(
                f"Could not find builtin transform {transform.type}"
            )
        tfm_args = _maybe_replace_dtype_arg(transform.args, return_spark)
        initTransform = tfm_ctor(**tfm_args)
    elif transform.name is not None:
        initTransform = _load_named_transform(
            policy, transform.name, registry, return_spark
        )
    else:
        raise ValueError(
            f"Expected type or name for transform with field {transform.field}"
        )
    return initTransform


def _load_named_transform(
    policy: data.Policy,
    transformLabel: str,
    registry: types.ModuleType,
    return_spark: bool,
):
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
            tfm_ctor = registry.get(transform.type)
            if tfm_ctor is None:
                raise exceptions.NamedTransformNotFound(
                    f"Could not find transform of type {transform.type} in registry"
                )
            tfm_args = _maybe_replace_dtype_arg(transform.args, return_spark)
            initTransform = tfm_ctor(**tfm_args)
            found = True
            break

    if not found:
        raise exceptions.NamedTransformNotFound(
            f"Could not find transform {transformLabel} in transformations block"
        )

    return initTransform


def parse_policy(p: str):
    """Parses a policy yaml file.

    The passed in string can either be a path to a local file or
    a URL pointing to a file. If it is a URL then requests attempts to download it.

    Args:
        p: a path string or a URL string

    Returns:
        The Policy object initialized by the yaml.
    """
    yaml_data: str

    if validators.url(p):
        yaml_data = requests.get(p).text
    else:
        with open(p) as f:
            yaml_data = f.read()

    policy = yaml.load(yaml_data, Loader=yaml.FullLoader)
    return data.Policy(**policy)
