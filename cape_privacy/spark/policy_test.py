import datetime

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
import yaml

from cape_privacy.pandas.transformations import test_utils
from cape_privacy.policy import data
from cape_privacy.policy import policy_test_fixtures as fixtures
from cape_privacy.spark import policy as plib
from cape_privacy.spark import registry
from cape_privacy.spark import test_utils as spark_utils
from cape_privacy.spark.transformations import base


def test_apply_policies():
    sess = spark_utils.make_session("test.policy.applyPolicies")
    pd_df = pd.DataFrame(np.ones(5,), columns=["test"])
    expected_df = pd_df + 3
    df = sess.createDataFrame(pd_df)

    registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = plib.apply_policies([p], "transactions", df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del registry._registry[test_utils.PlusN.identifier]


def test_apply_complex_policies():
    sess = spark_utils.make_session("test.policy.applyComplexPolicies")
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
    new_df = plib.apply_policies([p], "transactions", df).toPandas()
    pdt.assert_frame_equal(new_df, expected_df, check_dtype=True)


def test_named_transformation():
    sess = spark_utils.make_session("test.policy.namedTransformations")
    pd_df = pd.DataFrame(np.ones(5,), columns=["test"])
    expected_df = pd_df + 3
    df = sess.createDataFrame(pd_df)

    registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.named_y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = plib.apply_policies([p], "transactions", df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del registry._registry[test_utils.PlusN.identifier]


def test_redaction():
    sess = spark_utils.make_session("test.policy.redaction")
    pd_df = pd.DataFrame(np.ones((5, 2)), columns=["test", "apple"])
    pd_df["test"].iloc[0] = 6
    pd_df["test"].iloc[2] = 6
    expected_df = pd.DataFrame(np.ones(3,), columns=["test"])
    expected_df = expected_df + 3
    df = sess.createDataFrame(pd_df)

    registry.register(test_utils.PlusN.identifier, test_utils.PlusN)
    d = yaml.load(fixtures.redact_y, Loader=yaml.FullLoader)
    p = data.Policy(**d)
    new_df = plib.apply_policies([p], "transactions", df).toPandas()

    pdt.assert_frame_equal(new_df, expected_df)
    del registry._registry[test_utils.PlusN.identifier]
