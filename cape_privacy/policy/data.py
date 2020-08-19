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

from typing import List

import yaml

from cape_privacy.coordinator.utils import base64


class Transform:
    """A actual transform that will be applied.

    Either named or function must be passed in here. The process to apply this
    transform will look at both function and named and apply the relevant one.

    Attributes:
        field: The field this transform will be applied to.
        name: The name of the named transform, referenced from
              the top level policy object.
        type: The builtin transform that will be initialized.
        kwargs: The rest of the arguments that will be passed to the transformation.
    """

    def __init__(self, field, name=None, type=None, **kwargs):
        if field == "":
            raise ValueError("Field must be specified for transformation")

        if name is None and type is None:
            raise ValueError(
                "Either named or function must be specified"
                + f" for transformation on field {field}"
            )

        if name is not None and type is not None:
            raise ValueError(
                "Both named and function cannot be "
                + "fset for transformation on field {field}"
            )

        self.field = field
        self.name = name
        self.type = type
        self.args = kwargs


class Action:
    def __init__(self, field, transform=None):
        self.transform = Transform(field, **transform)


class Rule:
    """A rule contains actionable information of a policy.

    Attributes:
        match: The match used to select a field to be transformed.
        actions: The actions to take on a matched field.
    """

    def __init__(self, match, actions=[]):
        self.actions = []
        for action in actions:
            if type(action) is dict:
                self.actions.append(Action(match["name"], **action))
            # special case for dropping a column (i.e. column redaction)
            elif type(action) is str and action == "drop":
                self.actions.append(
                    Action(
                        match["name"],
                        {"type": "column-redact", "columns": [match["name"]]},
                    )
                )

        self.transformations = [action.transform for action in self.actions]


class NamedTransform:
    """A named transformation that captures the args.

    Attributes:
        name: The name of the named transformation.
        type: The builtin type (i.e. transform) that the named transform initializes to.
        kwargs: The args that are captured by the named transform.
    """

    def __init__(self, name, type, **kwargs):
        if name == "":
            raise ValueError("Name must be specified for named transformation")

        if type == "":
            raise ValueError(f"Type must be specified for named transformation {name}")

        if len(kwargs) == 0:
            raise ValueError(
                f"Args must be specified for named transformation {self.name}"
            )

        self.name = name
        self.type = type
        self.args = kwargs

        for key, arg in self.args.items():
            # if an arg is a secret
            if isinstance(arg, dict) and "type" in arg and arg["type"] == "secret":
                if "value" not in arg:
                    raise ValueError(
                        "Secret named transformation arg"
                        + f"{arg['name']} must contain a value"
                    )

                # then set the arg value to the inner value
                self.args[key] = bytes(base64.from_string(arg["value"]))


class Policy:
    """Top level policy object.

    The top level policy object holds the all of the relevant information
    for applying policy to data.

    Attributes:
        label: The label of the policy.
        version: The version of the policy.
        rules: List of rules that will be applied to a data frame.
        transformations: The named transformations for this policy.
    """

    def __init__(
        self,
        label: str = "",
        version: int = 1,
        rules: List[Rule] = [],
        transformations: List[NamedTransform] = [],
    ):
        self.label = label
        self.version = version

        self._raw_transforms = transformations
        self.transformations = [
            NamedTransform(**transform) for transform in transformations
        ]

        if len(rules) == 0:
            raise ValueError(
                f"At least one rule must be specified for policy specification {label}"
            )

        self._raw_rules = rules
        self.rules = [Rule(**rule) for rule in rules]

    def __repr__(self):
        d = {
            "label": self.label,
            "version": self.version,
            "transformations": self._raw_transforms,
            "rules": self._raw_rules,
        }

        return "Policy:\n\n" + yaml.dump(d, sort_keys=False)
