"""Contains the policy classes that are initialized from a yaml policy file.

There are five main classes with Policy being the top level class. Policy contains
the PolicySpec and NamedTransformations. PolicySpec contains Rules and Rules
contain Transformations.

    Typical usage example:

    yaml_str = "...."
    d = yaml.load(yaml_str, Loader=yaml.FullLoad)

    # **d unpacks the dictionary produced by yaml and
    # passes them in has keyword arguments.
    policy = Policy(**d)
"""
import pandas as pd

from cape_privacy import pandas as cape_pandas
from cape_privacy import spark as cape_spark
from cape_privacy.pandas import dtypes
from cape_privacy.policy import utils


class Policy:
    """Top level policy object.

    The top level policy object holds the policy label, policy spec
    and any named transformations.

    Attributes:
        label: The label of the policy.
        spec: The policy spec.
        transformations: The named transformations for this policy.
    """

    def __init__(self, label, spec=None, transformations=[]):
        self.label = label
        self.spec = PolicySpec(**spec)

        self.transformations = [
            NamedTransform(**transform) for transform in transformations
        ]

    def apply(self, df, entity: str):
        if cape_spark is not None and isinstance(df, cape_spark.DataFrame):
            return cape_spark.apply_policies([self], entity, df)
        elif isinstance(df, pd.DataFrame):
            return cape_pandas.apply_policies([self], entity, df)
        raise ValueError("Expected 'df' to be a DataFrame, found {}.".format(type(df)))


class PolicySpec:
    """Policy spec contains a list of rules.

    Attributes:
      label: The label of the policy spec. Often the same as the policy.
      rules: A list of rules.

    """

    def __init__(self, label, version=1, rules=[]):
        if label == "":
            raise ValueError("Label must be specified for policy specification")

        if len(rules) == 0:
            raise ValueError(
                f"At least one rule must be specified for policy specification {label}"
            )

        self.label = label
        self.version = version
        self.rules = [Rule(**rule) for rule in rules]


class Rule:
    """A rule contains actionable information of a policy.

    Attributes:
        target: The name of what this rule targets.
        effect: What effect the rule has. Currently only allow is supported.
        action: Which action the rule allows. Currently only read is supported.
        where: The clause that will redact rows if it
               evaluates to true (e.g. value == 10).
        transformations: A list of transformations that will be applied.
    """

    def __init__(
        self,
        target,
        effect="allow",
        action="read",
        where=None,
        redact=None,
        transformations=[],
    ):
        if target == "":
            raise ValueError("Target must be specified for rule")

        self.target = target
        self.effect = effect
        self.action = action
        self.where = where
        self.redact = redact

        self.transformations = [Transform(**transform) for transform in transformations]


class NamedTransform:
    """A named transformation that captures the args.

    Attributes:
        name: The name of the named transformation.
        type: The builtin type (i.e. transform) that the named transform initializes to.
        args: The args that are captured by the named transform.

    """

    def __init__(self, name, type, args={}):
        if name == "":
            raise ValueError("Name must be specified for named transformation")

        if type == "":
            raise ValueError(f"Type must be specified for named transformation {name}")

        if args == {}:
            raise ValueError(
                f"Args must be specified for named transformation {self.name}"
            )

        self.name = name
        self.type = type
        self.args = utils.yaml_args_to_kwargs(args)


class Transform:
    """A actual transform that will be applied.

    Either named or function must be passed in here. The process to apply this
    transform will look at both function and named and apply the relevant one.

    Attributes:
        field: The field this transform will be applied to.
        named: The name of the named transform, referenced from
               the top level policy object.
        function: The builtin transform that will be initialized.
        where: The clause that will apply this transform to the specified
               field if evaluated to true.
        args: The args that will be passed into the function if specified.
    """

    def __init__(self, field, named=None, function=None, where=None, args={}):
        if field == "":
            raise ValueError("Field must be specified for transformation")

        if named is None and function is None:
            raise ValueError(
                "Either named or function must be specified"
                + f" for transformation on field {field}"
            )

        if named is not None and function is not None:
            raise ValueError(
                "Both named and function cannot be "
                + "fset for transformation on field {field}"
            )

        self.field = field
        self.named = named
        self.function = function
        self.where = where
        self.args = utils.yaml_args_to_kwargs(args)
