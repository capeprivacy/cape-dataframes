import numpy as np
import pandas as pd
import pandas.testing as pdt

from .transformations import get


def test_plus_n():
    ctor = get("plusN")
    args = {"n": 1}
    transform = ctor("A", **args)

    df = pd.DataFrame(np.ones(5,), columns=["A"])
    expected = pd.DataFrame()

    expected["A"] = df["A"] + 1

    df["A"] = transform(df["A"])

    pdt.assert_frame_equal(df, expected)
