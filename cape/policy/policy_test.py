import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import requests
import yaml

from .data import Policy
from .exceptions import NamedTransformNotFound
from .policy import apply_policies
from .policy import parse_policy

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


def test_apply_policies():
    d = yaml.load(y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

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
