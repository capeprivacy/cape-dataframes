from pyspark import sql
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base
from cape_privacy.utils import typecheck


class NumericRounding(base.Transformation):
    """Reduce the precision of a numeric series

    Round each value in the series to the given number
    of digits.

    Attributes:
        dtypes (dtypes.Numerics): series type.
        precision (int): set the number of digits.
    """

    identifier = "numeric-rounding"
    type_signature = "col->col"

    def __init__(self, dtype: dtypes.DType, precision: int):
        if dtype not in dtypes.Numerics:
            raise ValueError("NumericRounding requires a Numeric dtype.")
        typecheck.check_arg(precision, int)
        super().__init__(dtype)
        self._precision = precision

    def __call__(self, x: sql.Column):
        return functions.round(x, scale=self._precision)


class DateTruncation(base.Transformation):
    """Reduce the precision of a date series

    Truncate each date in a series to the unit (year or month)
    specified by frequency.

    Attributes:
        frequency (string): expect to be 'year' or 'month'
    """

    identifier = "date-truncation"
    type_signature = "col->col"

    def __init__(self, frequency: str):
        typecheck.check_arg(frequency, str)
        super().__init__(dtypes.Date)
        self._frequency = frequency.lower()

    def __call__(self, x: sql.Column):
        truncated = functions.date_trunc(self._frequency, x)
        return truncated.astype(self.dtype)
