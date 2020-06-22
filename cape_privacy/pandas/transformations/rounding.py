import datetime

import pandas as pd

from cape_privacy.pandas.transformations import base
from cape_privacy.pandas.transformations import dtypes
from cape_privacy.utils import typecheck


class NumericRounding(base.Transformation):
    def __init__(self, dtype: dtypes.Numerics, precision: int):
        if dtype not in dtypes.Numerics:
            raise ValueError("NumericRounding requires a Numeric dtype.")
        typecheck.check_arg(precision, int)
        super().__init__(dtype)
        self._precision = precision

    def __call__(self, x: pd.Series):
        return self.round_numeric(x)

    def round_numeric(self, x: pd.Series):
        rounded = x.round(self._precision)
        if isinstance(rounded.dtype.type, self.dtype.type):
            return rounded
        else:
            return rounded.astype(self.dtype)


class DateTruncation(base.Transformation):
    def __init__(self, frequency: str):
        typecheck.check_arg(frequency, str)
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()
        _check_freq_arg(self._frequency)

    def __call__(self, x: pd.Series):
        return self._trunc_date(x)

    def _trunc_date(self, x: pd.Series):
        if self._frequency == "year":
            truncated = x.values.astype("<M8[Y]")
        elif self._frequency == "month":
            truncated = x.values.astype("<M8[M]")
        elif self._frequency == "day":
            truncated = x.values.astype("<M8[D]")
        elif self._frequency == "hour":
            truncated = x.values.astype("<M8[h]")
        elif self._frequency == "minute":
            truncated = x.values.astype("<M8[m]")
        elif self._frequency == "second":
            truncated = x.values.astype("<M8[s]")
        else:
            raise ValueError

        # Use equality instead of isintance because of inheritance
        if type(x[0]) == datetime.date:
            return pd.Series(truncated).dt.date
        else:
            return pd.Series(truncated)


def _check_freq_arg(arg):
    """Checks that arg is string or a flat collection of strings."""
    freq_options = ["year", "month", "day", "hour", "minute", "second"]

    if arg not in freq_options:
        raise ValueError("Frequency {} must be one of {}.".format(arg, freq_options,))
