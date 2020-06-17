from typing import Optional
from typing import Tuple
from typing import Union

import numpy as np
import pandas as pd

from cape_privacy.pandas.transformations import base
from cape_privacy.pandas.transformations import dtypes

_FREQUENCY_TO_DELTA_FN = {
    "YEAR": lambda noise: pd.Timedelta(days=noise * 365),
    "MONTH": lambda noise: pd.Timedelta(days=noise * 30),
    "DAY": lambda noise: pd.Timedelta(days=noise),
    "HOUR": lambda noise: pd.Timedelta(hours=noise),
    "minutes": lambda noise: pd.Timedelta(minutes=noise),
    "seconds": lambda noise: pd.Timedelta(seconds=noise),
}
IntTuple = Union[int, Tuple[int, ...]]
StrTuple = Union[str, Tuple[str, ...]]


class NumericPerturbation(base.Transformation):
    def __init__(
        self,
        dtype: dtypes.Numerics,
        min: (int, float),
        max: (int, float),
        seed: Optional[int] = None,
    ):
        super().__init__(dtype)
        self._min = min
        self._max = max
        self._rng = np.random.default_rng(seed=seed)

    def __call__(self, x: pd.Series):
        return self._perturb_numeric(x)

    def _perturb_numeric(self, x: pd.Series):
        noise = pd.Series(self._rng.uniform(self._min, self._max, size=x.shape))
        if not isinstance(noise.dtype.type, self.dtype.type):
            noise = noise.astype(self.dtype)
        return x + noise


class DatePerturbation(base.Transformation):
    def __init__(
        self,
        frequency: StrTuple,
        min: IntTuple,
        max: IntTuple,
        seed: Optional[int] = None,
    ):
        super().__init__(dtypes.Date)
        self._frequency = _check_freq_arg(frequency)
        self._min = _check_minmax_arg(min)
        self._max = _check_minmax_arg(max)
        self._rng = np.random.default_rng(seed)

    def __call__(self, series: pd.Series):
        return series.apply(lambda x: self._perturb_date(x))

    def _perturb_date(self, x: pd.Timestamp):
        for f, mn, mx in zip(self._frequency, self._min, self._max):
            noise = self._rng.integers(mn, mx)
            delta_fn = _FREQUENCY_TO_DELTA_FN.get(f, None)
            if delta_fn is None:
                raise ValueError(
                    "Frequency {} must be one of {}.".format(
                        f, list(_FREQUENCY_TO_DELTA_FN.keys())
                    )
                )
            x += delta_fn(noise)

        return x


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
