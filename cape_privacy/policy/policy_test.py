import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import requests
import yaml

from cape_privacy.pandas import policy as pd_policy
from cape_privacy.pandas import registry
from cape_privacy.pandas.transformations import test_utils
from cape_privacy.policy import data
from cape_privacy.policy import exceptions
from cape_privacy.policy import policy as policy_commons
from cape_privacy.policy import policy_test_fixtures as fixtures


def test_parse_policy(tmp_path):
    d = tmp_path / "policy"

    d.mkdir()

    p = d / "policy.yaml"
    p.write_text(fixtures.y)

    policy = policy_commons.parse_policy(str(p.absolute()))

    assert policy.label == "test_policy"


def test_named_transform_not_found():
    registry.register("plusN", test_utils.PlusN)
    d = yaml.load(
        fixtures.named_not_found_y("plusOne", "plusOneThousand", "plusN"),
        Loader=yaml.FullLoader,
    )

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    p = data.Policy(**d)
    tfm = p.spec.rules[0].transformations[0]

    with pytest.raises(exceptions.NamedTransformNotFound) as e:
        policy_commons.get_transformation(p, tfm, df, return_spark=False)

    assert (
        str(e.value)
        == "Could not find transform plusOneThousand in transformations block"
    )


def test_named_transform_type_not_found():
    d = yaml.load(
        fixtures.named_not_found_y("plusOne", "plusOne", "plusM"),
        Loader=yaml.FullLoader,
    )
    p = data.Policy(**d)
    tfm = p.spec.rules[0].transformations[0]
    df = pd.DataFrame(np.ones(5,), columns=["test"])

    with pytest.raises(exceptions.NamedTransformNotFound) as e:
        policy_commons.get_transformation(p, tfm, registry, return_spark=False)
    assert str(e.value) == "Could not find transform of type plusM in registry"


def test_parse_policy_url(httpserver):
    httpserver.expect_request("/policy").respond_with_data(fixtures.y)
    url = httpserver.url_for("/policy")
    policy = policy_commons.parse_policy(url)
    assert policy.label == "test_policy"


def test_parse_policy_invalid_url():
    with pytest.raises(requests.exceptions.ConnectionError):
        policy_commons.parse_policy("https://notapolicy.here.com/policy")


def test_parse_policy_invalid_file():
    with pytest.raises(FileNotFoundError):
        policy_commons.parse_policy("iamnotarealthingonthisfilesystem")
