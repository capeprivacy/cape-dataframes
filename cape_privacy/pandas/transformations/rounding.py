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
        rounded = round(x, self._precision)
        if isinstance(rounded.dtype.type, self.dtype.type):
            return rounded
        else:
            return rounded.astype(self.dtype)


class DateTruncation(base.Transformation):
    def __init__(self, frequency: str):
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()

    def __call__(self, series: pd.Timestamp):
        return series.apply(lambda x: self.round_date(x))

    def round_date(self, x):
        if self._frequency == "year":
            return pd.Timestamp(datetime.date(x.year, 1, 1))
        elif self._frequency == "month":
            return datetime.date(x.year, x.month, 1)
        else:
            raise ValueError
