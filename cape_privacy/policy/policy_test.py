import datetime

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import requests
import yaml

from cape_privacy import pandas as pandas_lib
from cape_privacy import spark as spark_lib
from cape_privacy.pandas.transformations import test_utils
from cape_privacy.policy import data
from cape_privacy.policy import exceptions
from cape_privacy.policy import policy as policy_lib
from cape_privacy.policy import policy_test_fixtures as fixtures


def test_parse_policy(tmp_path):
    d = tmp_path / "policy"

    d.mkdir()

    p = d / "policy.yaml"
    p.write_text(fixtures.y)

    policy = policy_lib.parse_policy(str(p.absolute()))

    assert policy.label == "test_policy"


def test_parse_policy_dict():
    p = yaml.load(fixtures.y, Loader=yaml.FullLoader)

    policy = policy_lib.parse_policy(p)

    assert policy.label == "test_policy"


def test_named_transform_not_found():
    pandas_lib.registry.register("plusN", test_utils.PlusN)
    d = yaml.load(
        fixtures.named_not_found_y("plusOne", "plusOneThousand", "plusN"),
        Loader=yaml.FullLoader,
    )

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    p = data.Policy(**d)
    tfm = p.rules[0].transformations[0]

    with pytest.raises(exceptions.NamedTransformNotFound) as e:
        policy_lib._get_transformation(p, tfm, df, pandas_lib.dtypes)

    assert str(e.value) == (
        "Could not find transform plusOneThousand in transformations block"
    )


def test_named_transform_type_not_found():
    d = yaml.load(
        fixtures.named_not_found_y("plusOne", "plusOne", "plusM"),
        Loader=yaml.FullLoader,
    )
    p = data.Policy(**d)
    tfm = p.rules[0].transformations[0]

    with pytest.raises(exceptions.NamedTransformNotFound) as e:
        policy_lib._get_transformation(p, tfm, pandas_lib.registry, pandas_lib.dtypes)
    assert str(e.value) == "Could not find transform of type plusM in registry"


def test_parse_policy_url(httpserver):
    httpserver.expect_request("/policy").respond_with_data(fixtures.y)
    url = httpserver.url_for("/policy")
    policy = policy_lib.parse_policy(url)
    assert policy.label == "test_policy"


def test_parse_policy_invalid_url():
    with pytest.raises(requests.exceptions.ConnectionError):
        policy_lib.parse_policy("https://notapolicy.here.com/policy")


def test_parse_policy_invalid_file():
    with pytest.raises(FileNotFoundError):
        policy_lib.parse_policy("iamnotarealthingonthisfilesystem")


def test_apply_policy_pandas():
    pandas_lib.registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = data.Policy(**d)

    new_df = policy_lib.apply_policy(p, df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_apply_complex_policies_pandas():
    d = yaml.load(fixtures.complex_y, Loader=yaml.FullLoader)

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

    p = data.Policy(**d)

    new_df = policy_lib.apply_policy(p, df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_named_transformation_pandas():
    pandas_lib.registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.named_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = data.Policy(**d)

    new_df = policy_lib.apply_policy(p, df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_column_redact_pandas():
    pandas_lib.registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.redact_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones((5, 2)), columns=["test", "apple"])

    p = data.Policy(**d)

    new_df = policy_lib.apply_policy(p, df)

    expected_df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = expected_df + 3

    pdt.assert_frame_equal(new_df, expected_df)


def test_apply_policy_spark():
    sess = spark_lib.utils.make_session("test.policy.applyPolicies")
    pd_df = pd.DataFrame(np.ones(5,), columns=["test"])
    expected_df = pd_df + 3
    df = sess.createDataFrame(pd_df)

    spark_lib.registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = policy_lib.apply_policy(p, df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del spark_lib.registry._registry[test_utils.PlusN.identifier]


def test_apply_complex_policies_spark():
    sess = spark_lib.utils.make_session("test.policy.applyComplexPolicies")
    pd_df = pd.DataFrame(
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
            "val-int": [25, 56],
            "val-float": [32.4, 56.6],
            # TODO: when these are pd.Timestamp, Spark's date_trunc is causing
            # dtype erasure. We should figure out why that's happening
            "date": [datetime.date(2018, 1, 1), datetime.date(2016, 1, 1)],
        }
    )
    df = sess.createDataFrame(pd_df)

    d = yaml.load(fixtures.complex_y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = policy_lib.apply_policy(p, df).toPandas()
    pdt.assert_frame_equal(new_df, expected_df, check_dtype=True)


def test_named_transformation_spark():
    sess = spark_lib.utils.make_session("test.policy.namedTransformations")
    pd_df = pd.DataFrame(np.ones(5,), columns=["test"])
    expected_df = pd_df + 3
    df = sess.createDataFrame(pd_df)

    spark_lib.registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.named_y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = policy_lib.apply_policy(p, df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del spark_lib.registry._registry[test_utils.PlusN.identifier]


def test_column_redaction_spark():
    sess = spark_lib.utils.make_session("test.policy.redaction")
    pd_df = pd.DataFrame(np.ones((5, 2)), columns=["test", "apple"])
    expected_df = pd.DataFrame(np.ones(5,), columns=["test"])
    expected_df = expected_df + 3
    df = sess.createDataFrame(pd_df)

    spark_lib.registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.redact_y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = policy_lib.apply_policy(p, df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del spark_lib.registry._registry[test_utils.PlusN.identifier]


def test_secret_in_named_transform():
    d = yaml.load(fixtures.secret_yaml, Loader=yaml.FullLoader)

    df = pd.DataFrame({"name": ["bob", "alice"]})

    p = data.Policy(**d)

    new_df = policy_lib.apply_policy(p, df)

    pdt.assert_frame_equal(new_df, df)


def test_reverse_helper():
    p = yaml.load(fixtures.reversible_yaml, Loader=yaml.FullLoader)

    policy = policy_lib.parse_policy(p)

    df = pd.DataFrame({"name": ["bob", "alice"]})

    new_df = policy_lib.apply_policy(policy, df)

    new_policy = policy_lib.reverse(policy)

    another_df = policy_lib.apply_policy(new_policy, new_df)

    for transform in new_policy.transformations:
        assert transform.type == pandas_lib.transformations.TokenReverser.identifier

    pdt.assert_frame_equal(df, another_df)
