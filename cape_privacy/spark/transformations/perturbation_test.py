import numpy as np
import pandas as pd
from pyspark import sql

from cape_privacy.spark import dtypes
from cape_privacy.spark import test_utils
from cape_privacy.spark.transformations import perturbation as ptb


def _make_and_apply_numeric_ptb(sess, df, dtype, min, max, seed=42):
    df = sess.createDataFrame(df, schema=["data"])
    perturb = ptb.NumericPerturbation(dtype, min=min, max=max, seed=seed)
    result_df = df.select(perturb(sql.functions.col("data")))
    return result_df.toPandas()


def test_float_ptb_bounds():
    sess = test_utils.make_session("test.perturbation.float.bounds")
    data = np.arange(6, dtype=np.float32).reshape((6, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower_bound = -2
    upper_bound = 2
    result_df = _make_and_apply_numeric_ptb(
        sess, test_df, dtypes.Float, lower_bound, upper_bound
    )
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower_bound
    upper_check = noise <= upper_bound
    assert lower_check.all()
    assert upper_check.all()


def test_double_ptb_bounds():
    sess = test_utils.make_session("test.perturbation.double.bounds")
    data = np.arange(6, dtype=np.float64).reshape((6, 1))
    test_df = pd.DataFrame(data, columns=["data"])
    lower = -2
    upper = 2
    result_df = _make_and_apply_numeric_ptb(sess, test_df, dtypes.Double, lower, upper)
    result = result_df.values
    assert result.dtype == data.dtype
    noise = result - data
    lower_check = noise >= lower
    upper_check = noise <= upper
    assert lower_check.all()
    assert upper_check.all()


# TODO test integer types

# TODO test date perturbation
