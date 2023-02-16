import datetime

import numpy as np
import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import DatePerturbation
from cape_privacy.pandas.transformations import NumericPerturbation


def test_perturbation_float():
    transform = NumericPerturbation(dtype=dtypes.Float, min=-10, max=10, seed=1234)

    df = pd.DataFrame({"amount": range(5)})
    expected = pd.DataFrame(
        {"amount": [9.53399, -1.39608, 10.46492, -1.76615, 0.38194]}
    )

    df["amount"] = transform(df.amount)

    pdt.assert_frame_equal(df, expected)


def test_perturbation_int():
    transform = NumericPerturbation(dtype=dtypes.Integer, min=-10, max=10, seed=12345)

    df = pd.DataFrame({"amount": range(5)})
    expected = pd.DataFrame({"amount": [-5, -2, 7, 6, 2]})

    df["amount"] = transform(df.amount)

    pdt.assert_frame_equal(df, expected)


def test_perturbation_datetime():
    transform = DatePerturbation(frequency="DAY", min=-10, max=10, seed=1234)

    df = pd.DataFrame({"date": [np.datetime64("2018-10-15")]})
    expected = pd.DataFrame({"date": [np.datetime64("2018-10-24")]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)


def test_perturbation_date():
    transform = DatePerturbation(frequency="DAY", min=-10, max=10, seed=1234)

    df = pd.DataFrame({"date": [datetime.date(year=2018, month=10, day=15)]})
    expected = pd.DataFrame({"date": [datetime.date(year=2018, month=10, day=24)]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)


def test_perturbation_dat_mutliple_freq():
    transform = DatePerturbation(
        frequency=("DAY", "YEAR"), min=(-10, -5), max=(10, 5), seed=1234
    )

    df = pd.DataFrame({"date": [np.datetime64("2018-10-15")]})
    expected = pd.DataFrame({"date": [np.datetime64("2022-10-23")]})

    df["date"] = transform(df.date)

    pdt.assert_frame_equal(df, expected)
