import datetime

import pandas as pd

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import base
from cape_privacy.utils import typecheck


class NumericRounding(base.Transformation):
    """Reduce the precision of a numeric Pandas Series

    Round each value in the Pandas Series to the given number
    of digits.

    Example:
        ```
        s = pd.Series([1.384])
        round = NumericRounding(precision=1)
        round(s) # pd.Series([1.4])
        ```

    Attributes:
        dtypes (dtypes.Numerics): Pandas Series type.
        precision (int): set the number of digits.
    """

    identifier = "numeric-rounding"
    type_signature = "col->col"

    def __init__(self, dtype: dtypes.Numerics, precision: int):
        if dtype not in dtypes.Numerics:
            raise ValueError("NumericRounding requires a Numeric dtype.")
        typecheck.check_arg(precision, int)
        super().__init__(dtype)
        self._precision = precision

    def __call__(self, x: pd.Series) -> pd.Series:
        """Round each value in the Pandas Series

        Args:
            x (A Pandas Series): need to be a list of numeric values.

        Return:
            A Pandas Series with each value rounded
        """
        return self.round_numeric(x)

    def round_numeric(self, x: pd.Series):
        rounded = x.round(self._precision)
        if isinstance(rounded.dtype.type, self.dtype.type):
            return rounded
        else:
            return rounded.astype(self.dtype)


class DateTruncation(base.Transformation):
    """Reduce the precision of a date Pandas Series
    Truncate each date in a Pandas Series to the unit (year
    or month) specified by frequency.
    Example:
        ```
        s = pd.Series([pd.Timestamp("2018-10-15")])
        trunc = DateTruncation(frequency="year")
        trunc(s) # pd.Serie([pd.Timestamp("2018-01-01")])
        ```
    Attributes:
        frequency (string): expect to be 'year' or 'month'
    """

    identifier = "date-truncation"
    type_signature = "col->col"

    def __init__(self, frequency: str):
        typecheck.check_arg(frequency, str)
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()
        _check_freq_arg(self._frequency)

    def __call__(self, x: pd.Series) -> pd.Series:
        return self._trunc_date(x)

    def _trunc_date(self, x: pd.Series) -> pd.Series:
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
        if type(x.iloc[0]) == datetime.date:
            return pd.Series(truncated).dt.date
        else:
            return pd.Series(truncated)


def _check_freq_arg(arg):
    """Checks that arg is string or a flat collection of strings."""
    freq_options = ["year", "month", "day", "hour", "minute", "second"]

    if arg not in freq_options:
        raise ValueError("Frequency {} must be one of {}.".format(arg, freq_options,))
