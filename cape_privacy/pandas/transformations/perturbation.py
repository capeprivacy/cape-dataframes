import datetime
from typing import Optional
from typing import Tuple
from typing import Union

import numpy as np
import pandas as pd

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import base
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
    """Add uniform random noise to a numeric Pandas series

    Mask a numeric Pandas series by adding uniform random
    noise to each value. The amount of noise is drawn from
    the interval [min, max).

    Example:
        ```
        s = pd.Series([0, 1, 2, 3, 4])
        perturb = NumericPerturbation(dtype=Integer, min=-10, max=10, seed=123)
        perturb(s) # pd.Series([3, -7, -3, -3])
        ```

    Attributes:
        dtype (dtypes.Numerics): Pandas Series type
        min (int, float): the values generated will be greater then or equal to min
        max (int, float): the values generated will be less than max
        seed (int), optional: a seed to initialize the random generator
    """

    identifier = "numeric-perturbation"
    type_signature = "col->col"

    def __init__(
        self,
        dtype: dtypes.Numerics,
        min: Union[int, float],
        max: Union[int, float],
        seed: Optional[int] = None,
    ):
        assert dtype in dtypes.Numerics
        typecheck.check_arg(min, (int, float))
        typecheck.check_arg(max, (int, float))
        typecheck.check_arg(seed, (int, type(None)))
        super().__init__(dtype)
        self._min = min
        self._max = max
        self._rng = np.random.default_rng(seed=seed)

    def __call__(self, x: pd.Series) -> pd.Series:
        noise = pd.Series(self._rng.uniform(self._min, self._max, size=x.shape))
        if not isinstance(noise.dtype.type, self.dtype.type):
            noise = noise.astype(self.dtype)
        return x + noise


class DatePerturbation(base.Transformation):
    """Add uniform random noise to a Pandas series of timestamps

    Mask a Pandas series by adding uniform random noise to the
    specified frequencies of timestamps. The amount of noise for
    each frequency is drawn from the internal [min_freq, max_freq).

    Example:
        ```
        s = pd.Series([datetime.date(year=2020, month=2, day=15)])
        perturb = DatePerturbation(frequency="MONTH", min=-10, max=10, seed=1234)
        perturb(s) # pd.Series([datetime.date(year=2020, month=11, day=11)])
        ```

    Attributes:
        frequency (str, str list): one or more frequencies to perturbate
        min (int, int list): the frequency value will be greater or equal to min
        max (int, int list): the frequency value will be less than max
        seed (int), optional: a seed to initialize the random generator
    """

    identifier = "date-perturbation"
    type_signature = "col->col"

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

    def __call__(self, x: pd.Series):
        is_date_no_time = False

        # Use equality instead of isinstance because of inheritance
        if type(x.iloc[0]) == datetime.date:
            x = pd.to_datetime(x)
            is_date_no_time = True

        for f, mn, mx in zip(self._frequency, self._min, self._max):
            noise = self._rng.integers(mn, mx, size=x.shape)
            delta_fn = _FREQUENCY_TO_DELTA_FN.get(f, None)
            if delta_fn is None:
                raise ValueError(
                    "Frequency {} must be one of {}.".format(
                        f, list(_FREQUENCY_TO_DELTA_FN.keys())
                    )
                )
            x += delta_fn(noise)

        if is_date_no_time:
            return pd.Series(x).dt.date
        else:
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
    """Checks that arg in one of the frequency options."""
    if not isinstance(arg, (tuple, list)):
        if not isinstance(arg, str):
            raise ValueError
        return [arg]
    else:
        for a in arg:
            if not isinstance(a, str):
                raise ValueError
    return arg
