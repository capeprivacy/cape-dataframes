import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas.transformations import ColumnRedact


def test_column_redact():
    redact = ColumnRedact(["b", "c"])

    df = pd.DataFrame(np.ones((5, 3)), columns=["a", "b", "c"])

    expected = pd.DataFrame(np.ones((5,)), columns=["a"])

    result = redact(df)

    pdt.assert_frame_equal(result, expected)
