import yaml

from cape_privacy.coordinator.utils import base64

from .data import Policy
from .policy_test_fixtures import named_with_secret_y

y = """label: test_policy
version: 1
transformations:
- name: plusOne
  type: plusN
  n: 1
rules:
- match:
    name: test
  actions:
  - transform:
      name: plusOne
  - transform:
      type: plusN
      n: 1
- match:
    name: test2
"""


def test_policy_class():
    d = yaml.load(y, Loader=yaml.FullLoader)

    p = Policy(**d)

    assert p.label == "test_policy"
    assert len(p.transformations) == 1

    named = p.transformations[0]
    assert named.name == "plusOne"
    assert named.type == "plusN"
    assert len(named.args) == 1

    assert named.args["n"] == 1

    rule = p.rules[0]
    assert len(p.rules) == 2
    assert len(rule.actions) == 2

    assert len(rule.transformations) == 2

    namedTransform = rule.transformations[0]
    builtinTransform = rule.transformations[1]

    assert namedTransform.field == "test"
    assert namedTransform.name == "plusOne"

    assert builtinTransform.field == "test"
    assert builtinTransform.type == "plusN"
    assert builtinTransform.args["n"] == 1


def test_policy_with_secret():
    d = yaml.load(named_with_secret_y, Loader=yaml.FullLoader)

    p = Policy(**d)

    assert p.transformations[1].args["key"] == bytes(base64.from_string("BASE"))


def test_policy_repr():
    d = yaml.load(y, Loader=yaml.FullLoader)

    p = Policy(**d)

    assert p.__repr__() == "Policy:\n\n" + y
