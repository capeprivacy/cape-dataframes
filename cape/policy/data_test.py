import yaml

from .data import Policy

y = """
    label: test_policy
    transformations:
      - name: plusOne
        type: plusN
        args:
          n:
            value: 1
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  named: plusOne
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 1
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

    spec = p.spec
    assert spec.version == 1
    assert spec.label == "test_policy"
    assert len(spec.rules) == 1

    rule = spec.rules[0]
    assert rule.target == "records:transactions.transactions"
    assert rule.action == "read"
    assert rule.effect == "allow"

    assert len(rule.transformations) == 2

    namedTransform = rule.transformations[0]
    builtinTransform = rule.transformations[1]

    assert namedTransform.field == "test"
    assert namedTransform.named == "plusOne"

    assert builtinTransform.field == "test"
    assert builtinTransform.function == "plusN"
    assert builtinTransform.args["n"] == 1
