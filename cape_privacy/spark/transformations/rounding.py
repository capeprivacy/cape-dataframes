from pyspark import sql
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base


class Rounding(base.Transformation):
    def __init__(self, dtype: dtypes.DType, precision: int):
        assert dtype in dtypes.Numerics
        super().__init__(dtype)
        self._precision = precision

    def __call__(self, x: sql.Column):
        rounded = functions.round(x, scale=self._precision)
        # functions.round automatically upcasts to high precision (eg. double)
        # so we truncate back down to the original dtype.
        return rounded.astype(self.dtype)


class DateTruncation(base.Transformation):
    def __init__(self, frequency: str):
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()

    def __call__(self, x: sql.Column):
        truncated = functions.date_trunc(self._frequency, x)
        return truncated.astype(self.dtype)
