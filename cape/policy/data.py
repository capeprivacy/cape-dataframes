from .utils import yaml_args_to_kwargs


class Policy:
    def __init__(self, label="", spec=None, transformations=[]):
        self.label = label
        self.spec = PolicySpec(**spec)

        self.transformations = [
            NamedTransform(**transform) for transform in transformations
        ]


class PolicySpec:
    def __init__(self, label="", version=1, rules=[]):
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
    def __init__(self, target="", effect="allow", action="read", transformations=[]):
        if target == "":
            raise ValueError("Target must be specified for rule")

        self.target = target
        self.effect = effect
        self.action = action

        self.transformations = [Transform(**transform) for transform in transformations]


class NamedTransform:
    def __init__(self, name="", type="", args={}):
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
        self.args = yaml_args_to_kwargs(args)


class Transform:
    def __init__(self, field="", named="", function="", args={}):
        if field == "":
            raise ValueError("Field must be specified for transformation")

        if named == "" and function == "":
            raise ValueError(
                "Either named or function must be specified"
                + f" for transformation on field {field}"
            )

        if named != "" and function != "":
            raise ValueError(
                "Both named and function cannot be "
                + "fset for transformation on field {field}"
            )

        self.field = field
        self.named = named
        self.function = function
        self.args = yaml_args_to_kwargs(args)
