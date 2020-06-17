import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas.transformations import DateTruncation
from cape_privacy.pandas.transformations import NumericRounding
from cape_privacy.pandas.transformations import dtypes


def test_rounding_float32():
    transform = NumericRounding(dtype=dtypes.Float, precision=1)

    df = pd.DataFrame({"amount": [10.8834, 4.21221]}).astype(np.float32)
    expected = pd.DataFrame({"amount": [10.9, 4.2]}).astype(np.float32)

    df["amount"] = transform(df.amount)

    pdt.assert_frame_equal(df, expected)


def test_rounding_float64():
    transform = NumericRounding(dtype=dtypes.Double, precision=1)

    df = pd.DataFrame({"amount": [10.8834, 4.21221]}).astype(np.float64)
    expected = pd.DataFrame({"amount": [10.9, 4.2]}).astype(np.float64)

    df["amount"] = transform(df.amount)

    pdt.assert_frame_equal(df, expected)


def test_rounding_date():
    transform = DateTruncation(frequency="YEAR")

    df = pd.DataFrame({"date": [pd.Timestamp("2018-10-15")]})
    expected = pd.DataFrame({"date": [pd.Timestamp("2018-01-01")]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)
