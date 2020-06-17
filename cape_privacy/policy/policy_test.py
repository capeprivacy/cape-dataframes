import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import requests
import yaml

from cape_privacy.pandas.transformations.test_utils import PlusN
from cape_privacy.pandas.transformations.transformations import register
from cape_privacy.policy import NamedTransformNotFound
from cape_privacy.policy import Policy
from cape_privacy.policy import apply_policies
from cape_privacy.policy import parse_policy

y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 1
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 2
    """

named_y = """
    label: test_policy
    transformations:
      - name: plusOne
        type: plusN
        args:
          n:
            value: 1
      - name: plusTwo
        type: plusN
        args:
          n:
            value: 2
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
                  named: plusTwo
    """

named_not_found_y = """
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
                  named: plusOneThousand
"""

complex_y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: val-int
                  function: numeric-perturbation
                  args:
                    dtype:
                      value: Integer
                    min:
                      value: -10
                    max:
                      value: 10
                    seed:
                      value: 4984
                - field: val-float
                  function: numeric-rounding
                  args:
                    dtype:
                      value: Double
                    precision:
                      value: 1
                - field: name
                  function: tokenizer
                  args:
                    key:
                      value: secret_key
                - field: date
                  function: date-truncation
                  args:
                    frequency:
                      value: year
    """


redact_y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              redact:
                - apple
              where: test > 2
              transformations:
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 1
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 2
    """

register("plusN", PlusN)


def test_apply_policies():
    d = yaml.load(y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = Policy(**d)

    new_df = apply_policies([p], "transactions", df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_apply_complex_policies():
    d = yaml.load(complex_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(
        {
            "name": ["bob", "alice"],
            "val-int": [30, 50],
            "val-float": [32.43424, 56.64543],
            "date": [pd.Timestamp("2018-10-15"), pd.Timestamp("2016-09-10")],
        }
    )
    expected_df = pd.DataFrame(
        {
            "name": [
                "db6063546d5d6c1fd3826bc0a1d8188fa0dae1a174823eac1e8e063a073bf149",
                "4ae0639267ad49c658e8d266aa1caa51c876ed1d7ca788a0749d5189248295eb",
            ],
            "val-int": [23, 58],
            "val-float": [32.4, 56.6],
            "date": [pd.Timestamp("2018-01-01"), pd.Timestamp("2016-01-01")],
        }
    )

    p = Policy(**d)

    new_df = apply_policies([p], "transactions", df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_parse_policy(tmp_path):
    d = tmp_path / "policy"

    d.mkdir()

    p = d / "policy.yaml"
    p.write_text(y)

    policy = parse_policy(str(p.absolute()))

    assert policy.label == "test_policy"


def test_named_transformation():
    d = yaml.load(named_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = Policy(**d)

    new_df = apply_policies([p], "transactions", df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_named_transform_not_found():
    d = yaml.load(named_not_found_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    p = Policy(**d)

    with pytest.raises(NamedTransformNotFound) as e:
        apply_policies([p], "transactions", df)

    assert (
        str(e.value)
        == "Could not find transform plusOneThousand in transformations block"
    )


def test_parse_policy_url(httpserver):
    httpserver.expect_request("/policy").respond_with_data(y)
    url = httpserver.url_for("/policy")
    policy = parse_policy(url)
    assert policy.label == "test_policy"


def test_parse_policy_invalid_url():
    with pytest.raises(requests.exceptions.ConnectionError):
        parse_policy("https://notapolicy.here.com/policy")


def test_parse_policy_invalid_file():
    with pytest.raises(FileNotFoundError):
        parse_policy("iamnotarealthingonthisfilesystem")


def test_redact():
    d = yaml.load(redact_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones((5, 2)), columns=["test", "apple"])

    df["test"].iloc[0] = 6
    df["test"].iloc[2] = 6

    p = Policy(**d)

    new_df = apply_policies([p], "transactions", df)

    expected_df = pd.DataFrame(np.ones(3,), columns=["test"], index=[1, 3, 4])

    expected_df = expected_df + 3

    pdt.assert_frame_equal(new_df, expected_df)
