import datetime

import pandas as pd

from cape_privacy.pandas.transformations import base
from cape_privacy.pandas.transformations import dtypes


class NumericRounding(base.Transformation):
    def __init__(self, dtype: dtypes.Numerics, precision: int):
        if dtype not in dtypes.Numerics:
            raise ValueError("NumericRounding requires a Numeric dtype.")
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
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()

    def __call__(self, x: pd.Series):
        return self.trunc_date(x)

    def trunc_date(self, x):
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
            raise ValueError(
                "Frequency {} must be one of {}.".format(
                    self._frequency,
                    list(["YEAR", "MONTH", "DAY", "hour", "minute", "second"]),
                )
            )

        # Use equality instead of isintance because of inheritance
        if type(x[0]) == datetime.date:
            return pd.Series(truncated).dt.date
        else:
            return pd.Series(truncated)
