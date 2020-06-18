import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas.transformations import RowRedact


def test_row_redact():
    redact = RowRedact("a > 5")

    df = pd.DataFrame(np.ones((5, 2)), columns=["a", "b"])

    df["a"].iloc[0] = 6
    df["a"].iloc[3] = 6

    expected = pd.DataFrame(np.ones((3, 2)), columns=["a", "b"], index=[1, 2, 4])

    result = redact(df)

    pdt.assert_frame_equal(result, expected)
