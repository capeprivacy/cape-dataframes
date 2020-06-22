import datetime

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


def test_truncate_date():
    transform = DateTruncation(frequency="YEAR")

    df = pd.DataFrame({"date": [datetime.date(year=2018, month=10, day=3)]})
    expected = pd.DataFrame({"date": [datetime.date(year=2018, month=1, day=1)]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_year():
    transform = DateTruncation(frequency="YEAR")

    df = pd.DataFrame({"date": [pd.Timestamp(year=2018, month=10, day=3)]})
    expected = pd.DataFrame({"date": [pd.Timestamp(year=2018, month=1, day=1)]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_min():
    transform = DateTruncation(frequency="minute")

    date = datetime.datetime(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    df = pd.DataFrame({"date": [date]})
    expected_date = datetime.datetime(
        year=2018, month=10, day=3, hour=9, minute=20, second=0
    )
    expected = pd.DataFrame({"date": [expected_date]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_hour():
    transform = DateTruncation(frequency="hour")

    date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    df = pd.DataFrame({"date": [date]})
    expected_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=0, second=0)
    expected = pd.DataFrame({"date": [expected_date]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)
