import datetime

import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import DateTruncation
from cape_privacy.pandas.transformations import NumericRounding


def _make_apply_numeric_rounding(input, expected_output, ctype, dtype):
    transform = NumericRounding(dtype=ctype, precision=1)
    df = pd.DataFrame({"amount": input}).astype(dtype)
    expected = pd.DataFrame({"amount": expected_output}).astype(dtype)
    df["amount"] = transform(df.amount)
    return df, expected


def _make_apply_datetruncation(frequency, input_date, expected_date):
    transform = DateTruncation(frequency=frequency)
    df = pd.DataFrame({"date": [input_date]})
    expected = pd.DataFrame({"date": [expected_date]})
    df["date"] = transform(df.date)
    return df, expected


def test_rounding_float32():
    input = [10.8834, 4.21221]
    expected_output = [10.9, 4.2]
    df, expected = _make_apply_numeric_rounding(
        input, expected_output, dtypes.Float, np.float32
    )
    pdt.assert_frame_equal(df, expected)


def test_rounding_float64():
    input = [10.8834, 4.21221]
    expected_output = [10.9, 4.2]
    df, expected = _make_apply_numeric_rounding(
        input, expected_output, dtypes.Double, np.float64
    )
    pdt.assert_frame_equal(df, expected)


def test_truncate_date_year():
    input_date = datetime.date(year=2018, month=10, day=3)
    expected_date = datetime.date(year=2018, month=1, day=1)
    df, expected = _make_apply_datetruncation("YEAR", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_year():
    input_date = pd.Timestamp(year=2018, month=10, day=3)
    expected_date = pd.Timestamp(year=2018, month=1, day=1)
    df, expected = _make_apply_datetruncation("YEAR", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_month():
    input_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    expected_date = pd.Timestamp(year=2018, month=10, day=1, hour=0, minute=0, second=0)
    df, expected = _make_apply_datetruncation("MONTH", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_day():
    input_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    expected_date = pd.Timestamp(year=2018, month=10, day=3, hour=0, minute=0, second=0)
    df, expected = _make_apply_datetruncation("DAY", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_hour():
    input_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    expected_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=0, second=0)
    df, expected = _make_apply_datetruncation("hour", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_minute():
    input_date = datetime.datetime(
        year=2018, month=10, day=3, hour=9, minute=20, second=25
    )
    expected_date = datetime.datetime(
        year=2018, month=10, day=3, hour=9, minute=20, second=0
    )
    df, expected = _make_apply_datetruncation("minute", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)


def test_truncate_datetime_second():
    input_date = pd.Timestamp(year=2018, month=10, day=3, hour=9, minute=20, second=25)
    expected_date = pd.Timestamp(
        year=2018, month=10, day=3, hour=9, minute=20, second=25
    )
    df, expected = _make_apply_datetruncation("second", input_date, expected_date)
    pdt.assert_frame_equal(df, expected)
