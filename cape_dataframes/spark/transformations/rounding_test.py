import datetime

import numpy as np
import pandas as pd
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark import utils
from cape_privacy.spark.transformations import rounding as rnd


# Utils
def _make_and_apply_rounder(sess, df, dtype, precision):
    df = sess.createDataFrame(df, schema=["data"])
    rounder = rnd.NumericRounding(dtype, precision)
    result_df = df.select(rounder(functions.col("data")))
    return result_df.toPandas()


def _make_float_data(dtype, precision=0, scale=0.1):
    data = np.arange(6, dtype=dtype).reshape((6, 1))
    delta = data * scale
    expected = np.around(data + delta, decimals=precision)
    test_df = pd.DataFrame(data + delta, columns=["data"])
    return test_df, expected


def _make_integer_data(dtype, precision):
    data = np.array([123, 1234, 12345, 123456], dtype=dtype).reshape((4, 1))
    expected = np.around(data, precision)
    test_df = pd.DataFrame(data, columns=["data"])
    return test_df, expected


def _make_date_data(sess):
    df = sess.createDataFrame([("1997-02-28",)], ["data"])
    expected = np.array(datetime.date(1997, 2, 1))
    return df, expected


def _make_datetime_data(sess):
    df = sess.createDataFrame([("1997-02-28 05:02:11",)], ["data"])
    expected = np.array(datetime.datetime(1997, 2, 1, 0, 0, 0))
    return df, expected


# Tests
def test_rounding_float():
    precision = 0
    sess = utils.make_session("test.rounding.float")
    test_df, expected = _make_float_data(np.float32, precision)
    result_df = _make_and_apply_rounder(sess, test_df, dtypes.Float, precision)
    result = result_df.values
    assert result.dtype == expected.dtype
    np.testing.assert_almost_equal(result, expected)


def test_rounding_double():
    precision = 0
    sess = utils.make_session("test.rounding.double")
    test_df, expected = _make_float_data(np.float64, precision)
    result_df = _make_and_apply_rounder(sess, test_df, dtypes.Double, precision)
    result = result_df.values
    assert result.dtype == expected.dtype
    np.testing.assert_almost_equal(result, expected)


def test_rounding_integer():
    precision = -2
    sess = utils.make_session("test.rounding.integer")
    test_df, expected = _make_integer_data(np.int32, precision)
    result_df = _make_and_apply_rounder(sess, test_df, dtypes.Integer, precision)
    result = result_df.values
    assert result.dtype == expected.dtype
    np.testing.assert_almost_equal(result, expected)


def test_rounding_long():
    precision = -2
    sess = utils.make_session("test.rounding.integer")
    test_df, expected = _make_integer_data(np.int64, precision)
    result_df = _make_and_apply_rounder(sess, test_df, dtypes.Long, precision)
    result = result_df.values
    assert result.dtype == expected.dtype
    np.testing.assert_almost_equal(result, expected)


def test_truncate_date():
    sess = utils.make_session("test.truncation.date")
    test_df, expected = _make_date_data(sess)
    truncate = rnd.DateTruncation("month")
    result_df = test_df.select(truncate(test_df.data)).toPandas()
    result = result_df.values
    assert result.dtype == expected.dtype
    np.testing.assert_equal(result, expected)
