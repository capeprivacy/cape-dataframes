from typing import Optional

import numpy as np
import pandas as pd
from pyspark import sql
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base


_FREQUENCY_TO_DELTA_FN = {
    'YEAR': lambda noise: pd.Timedelta(days=noise * 365),
    'MONTH': lambda noise: pd.Timedelta(days=noise * 30),
    'DAY': lambda noise: pd.Timedelta(days=noise),
    'HOUR': lambda noise: pd.Timedelta(hours=noise),
    'minutes': lambda noise: pd.Timedelta(minutes=noise),
    'seconds': lambda noise: pd.Timedelta(seconds=noise),
}

class NumericPerturbation(base.Transformation):
    def __init__(
        self,
        dtype: dtypes.DType,
        min: (int, float),
        max: (int, float),
        seed: Optional[int] = None,
    ):
        assert dtype in dtypes.Numerics
        super().__init__(dtype)
        self._min = min
        self._max = max
        self._seed = seed

    def __call__(self, x: sql.Column):
        uniform_noise = functions.rand(seed=self._seed)
        affine_noise = self._min + uniform_noise * (self._max - self._min)
        if self._dtype is not dtypes.Double:
            affine_noise = affine_noise.astype(self._dtype)
        return x + affine_noise


class DatePerturbation(base.Transformation):
    def __init__(self, frequency, min, max, seed=None):
        super().__init__(dtypes.Date)
        self._frequency = _check_str_arg(frequency)
        self._min = _check_int_arg(min)
        self._max = _check_int_arg(max)
        self._rng = np.random.default_rng(seed)

    def __call__(self, x: sql.Column):
        return self.perturb_date(x)

    @functions.pandas_udf(dtypes.Date, functions.PandasUDFType.SCALAR)
    def perturb_date(self, x: pd.Series) -> pd.Series:
        for f, mn, mx in zip(self._frequency, self._min, self._max):
            # TODO can we switch to a lower dtype than np.int64?
            noise = self._rng.integers(self._min, self._max, size=x.shape)
            delta_fn = _FREQUENCY_TO_DELTA_FN.get(f, None)
            if delta_fn is None:
                raise ValueError("Frequency {} must be one of {}.".format(
                    f, list(_FREQUENCY_TO_DELTA_FN.keys())))
            x += delta_fn(noise)
        return x


def _check_int_arg(arg):
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

def _check_str_arg(arg):
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
