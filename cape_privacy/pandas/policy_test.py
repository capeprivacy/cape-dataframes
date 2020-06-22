import numpy as np
import pandas as pd
import pandas.testing as pdt
import yaml

from cape_privacy.pandas import policy as plib
from cape_privacy.pandas import registry
from cape_privacy.pandas.transformations import test_utils
from cape_privacy.policy import data
from cape_privacy.policy import policy_test_fixtures as fixtures


def test_apply_policies():
    registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = data.Policy(**d)

    new_df = plib.apply_policies([p], df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_apply_complex_policies():
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

    new_df = plib.apply_policies([p], df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_named_transformation():
    registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.named_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = df + 3

    p = data.Policy(**d)

    new_df = plib.apply_policies([p], df)

    pdt.assert_frame_equal(new_df, expected_df)


def test_column_redact():
    registry.register("plusN", test_utils.PlusN)
    d = yaml.load(fixtures.redact_y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones((5, 2)), columns=["test", "apple"])

    p = data.Policy(**d)

    new_df = plib.apply_policies([p], df)

    expected_df = pd.DataFrame(np.ones(5,), columns=["test"])

    expected_df = expected_df + 3

    pdt.assert_frame_equal(new_df, expected_df)
