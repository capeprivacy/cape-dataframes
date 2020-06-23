from typing import Optional
from typing import Tuple
from typing import Union

import numpy as np
import pandas as pd
from pyspark import sql
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base
from cape_privacy.utils import typecheck

_FREQUENCY_TO_DELTA_FN = {
    "YEAR": lambda noise: pd.to_timedelta(noise * 365, unit="days"),
    "MONTH": lambda noise: pd.to_timedelta(noise * 30, unit="days"),
    "DAY": lambda noise: pd.to_timedelta(noise, unit="days"),
    "HOUR": lambda noise: pd.to_timedelta(noise, unit="hours"),
    "minutes": lambda noise: pd.to_timedelta(noise, unit="minutes"),
    "seconds": lambda noise: pd.to_timedelta(noise, unit="seconds"),
}
IntTuple = Union[int, Tuple[int, ...]]
StrTuple = Union[str, Tuple[str, ...]]


class NumericPerturbation(base.Transformation):
    """Add uniform random noise to a numeric series

    Mask a numeric series by adding uniform random noise to each value.
    The amount of noise is drawn from the interval [min, max).

    Attributes:
        dtype (dtypes.Numerics): series type
        min (int, float): the values generated will be greater or equal to min
        max (int, float): the values generated will be less than max
        seed (int), optional: a seed to initialize the random generator
    """

    identifier = "numeric-perturbation"
    type_signature = "col->col"

    def __init__(
        self,
        dtype: dtypes.DType,
        min: (int, float),
        max: (int, float),
        seed: Optional[int] = None,
    ):
        assert dtype in dtypes.Numerics
        typecheck.check_arg(min, (int, float))
        typecheck.check_arg(max, (int, float))
        typecheck.check_arg(seed, (int, type(None)))
        super().__init__(dtype)
        self._min = min
        self._max = max
        self._seed = seed

    def __call__(self, x: sql.Column):
        uniform_noise = functions.rand(seed=self._seed)
        if self._seed is not None:
            self._seed += 1
        affine_noise = self._min + uniform_noise * (self._max - self._min)
        if self._dtype is not dtypes.Double:
            affine_noise = affine_noise.astype(self._dtype)
        return x + affine_noise


class DatePerturbation(base.Transformation):
    """Add uniform random noise to a Pandas series of timestamps

    Mask a series by adding uniform random noise to the specified
    frequencies of timestamps. The amount of noise for each frequency
    is drawn from the internal [min_freq, max_freq).

    Note that seeds are currently not supported.

    Attributes:
        frequency (str, str list): one or more frequencies to perturbate
        min (int, int list): the frequency value will be greater or equal to min
        max (int, int list): the frequency value will be less than max
    """

    identifier = "date-perturbation"
    type_signature = "col->col"

    def __init__(
        self, frequency: StrTuple, min: IntTuple, max: IntTuple,
    ):
        super().__init__(dtypes.Date)
        self._frequency = _check_freq_arg(frequency)
        self._min = _check_minmax_arg(min)
        self._max = _check_minmax_arg(max)
        self._perturb_date = None

    def __call__(self, x: sql.Column):
        if self._perturb_date is None:
            self._perturb_date = self._make_perturb_udf()
        return self._perturb_date(x)

    def _make_perturb_udf(self):
        @functions.pandas_udf(dtypes.Date, functions.PandasUDFType.SCALAR)
        def perturb_date(x: pd.Series) -> pd.Series:
            rng = np.random.default_rng()
            for f, mn, mx in zip(self._frequency, self._min, self._max):
                # TODO can we switch to a lower dtype than np.int64?
                noise = rng.integers(mn, mx, size=x.shape)
                delta_fn = _FREQUENCY_TO_DELTA_FN.get(f, None)
                if delta_fn is None:
                    raise ValueError(
                        "Frequency {} must be one of {}.".format(
                            f, list(_FREQUENCY_TO_DELTA_FN.keys())
                        )
                    )
                x += delta_fn(noise)
            return x

        return perturb_date


def _check_minmax_arg(arg):
    """Checks that arg is an integer or a flat collection of integers."""
    if not isinstance(arg, (tuple, list)):
        if not isinstance(arg, int):
            raise ValueError
        return [arg]
    else:
        for a in arg:
            if not isinstance(a, int):
                raise ValueError
    return arg


def _check_freq_arg(arg):
    """Checks that arg is string or a flat collection of strings."""
    if not isinstance(arg, (tuple, list)):
        if not isinstance(arg, str):
            raise ValueError
        return [arg]
    else:
        for a in arg:
            if not isinstance(a, str):
                raise ValueError
    return arg
