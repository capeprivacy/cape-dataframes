import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.spark import utils
from cape_privacy.spark.transformations import redaction as rdc


def test_column_redact():
    sess = utils.make_session("test.redaction.column")
    df = pd.DataFrame(np.ones((5, 3)), columns=["a", "b", "c"])
    expected = pd.DataFrame(np.ones((5,)), columns=["a"])
    test_df = sess.createDataFrame(df, schema=["a", "b", "c"])
    redact = rdc.ColumnRedact(["b", "c"])
    result = redact(test_df).toPandas()
    pdt.assert_frame_equal(result, expected)


def test_row_redact():
    sess = utils.make_session("test.redaction.row")
    df = pd.DataFrame(np.ones((5, 2)), columns=["a", "b"])
    df["a"].iloc[0] = 6
    df["a"].iloc[3] = 6
    expected = pd.DataFrame(np.ones((3, 2)), columns=["a", "b"])
    test_df = sess.createDataFrame(df, schema=["a", "b"])
    redact = rdc.RowRedact("a > 5")
    result = redact(test_df).toPandas()
    pdt.assert_frame_equal(result, expected)
