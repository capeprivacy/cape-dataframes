import numpy as np
import pandas as pd
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark import utils
from cape_privacy.spark.transformations import perturbation as ptb


def _make_and_apply_numeric_ptb(sess, df, dtype, min, max):
    df = sess.createDataFrame(df, schema=["data"])
    perturb = ptb.NumericPerturbation(dtype, min=min, max=max)
    result_df = df.select(perturb(functions.col("data")))
    return result_df.toPandas()


def _make_and_apply_date_ptb(sess, df, frequency, min, max):
    df = sess.createDataFrame(df, schema=["data"])
    perturb = ptb.DatePerturbation(frequency, min, max)
    result_df = df.select(perturb(functions.col("data")))
    return result_df.withColumnRenamed("perturb_date(data)", "data").toPandas()


def test_float_ptb_bounds():
    sess = utils.make_session("test.perturbation.float.bounds")
    data = np.arange(6, dtype=np.float32).reshape((6, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -2, 2
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Float, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_double_ptb_bounds():
    sess = utils.make_session("test.perturbation.double.bounds")
    data = np.arange(6, dtype=np.float64).reshape((6, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -2, 2
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Double, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_int_ptb_bounds():
    sess = utils.make_session("test.perturbation.integer.bounds")
    data = np.arange(10, dtype=np.int32).reshape((10, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -3, 3
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Integer, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_byte_ptb_bounds():
    sess = utils.make_session("test.perturbation.byte.bounds")
    data = np.arange(10, dtype=np.int8).reshape((10, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -3, 3
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Byte, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_short_ptb_bounds():
    sess = utils.make_session("test.perturbation.short.bounds")
    data = np.arange(10, dtype=np.int16).reshape((10, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -3, 3
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Short, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_integer_ptb_bounds():
    sess = utils.make_session("test.perturbation.integer.bounds")
    data = np.arange(10, dtype=np.int32).reshape((10, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -3, 3
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Integer, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_long_ptb_bounds():
    sess = utils.make_session("test.perturbation.long.bounds")
    data = np.arange(10, dtype=np.int64).reshape((10, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower, upper = -3, 3
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Long, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


def test_date_perturbation_singlefreq_bounds():
    sess = utils.make_session("test.perturbation.date.bounds.singleFrequency")
    data = pd.to_datetime(["1997-03-15", "2020-06-24"])
    test_df = pd.DataFrame(data, columns=["data"])
    frequencies = ["YEAR", "MONTH", "DAY"]
    num_days = [365, 30, 1]
    lower, upper = -2, 2
    for freq, days in zip(frequencies, num_days):
        result_df = _make_and_apply_date_ptb(sess, test_df, freq, lower, upper)
        result_df = result_df.apply(pd.to_datetime)
        noise_df = result_df - test_df
        lower_check = noise_df >= pd.to_timedelta(lower * days, unit="days")
        upper_check = noise_df <= pd.to_timedelta(upper * days, unit="days")
        assert lower_check.values.all()
        assert upper_check.values.all()


def test_date_perturbation_multifreq_bounds():
    sess = utils.make_session("test.perturbation.date.bounds.singleFrequency")
    data = pd.to_datetime(["1997-03-15", "2020-06-24"])
    test_df = pd.DataFrame(data, columns=["data"])
    frequency = ("MONTH", "DAY")
    lower, upper = (-1, -30), (1, 30)
    result_df = _make_and_apply_date_ptb(sess, test_df, frequency, lower, upper)
    result_df = result_df.apply(pd.to_datetime)
    noise_df = result_df - test_df
    lower_check = noise_df >= pd.to_timedelta(-60, unit="days")
    upper_check = noise_df <= pd.to_timedelta(60, unit="days")
    assert lower_check.values.all()
    assert upper_check.values.all()
