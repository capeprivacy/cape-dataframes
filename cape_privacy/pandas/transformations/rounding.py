import datetime
import numpy as np

from cape_pandas import types
from cape_pandas.transformations import base


class Rounding(base.Transformation):
    def __init__(self, input_type, **type_kwargs):
        super().__init__(input_type)
        self._type_kwargs = type_kwargs
        if self.type == types.Date:
            self._caller = self.round_date
        elif self.type in types.Numerics:
            self._caller = self.round_numeric
        else:
            raise ValueError

    def __call__(self, x):
        return self._caller(x, **self._type_kwargs)

    def round_numeric(self, x, number_digits):
        return round(x, number_digits)

    def round_date(self, x, frequency):
        # [NOTE] should be reviewed to match a SQL round
        # https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions136.htm
        if frequency.upper() == 'YEAR':
            return datetime.date(x.year, 1, 1)
        elif frequency.upper() == 'MONTH':
            return datetime.date(x.year, x.month, 1)
        else:
            raise ValueError


class NativeRounding(base.Transformation):
    def __init__(self, input_type, **type_kwargs):
        super().__init__(input_type)
        self._type_kwargs = type_kwargs
        if self.type == types.Date:
            self._caller = self.round_date
        elif self.type in types.Numerics:
            self._caller = self.round_numeric
        else:
            raise ValueError

    def __call__(self, x):
        return self._caller(x, **self._type_kwargs)

    def round_numeric(self, x, number_digits):
        return np.around(x, decimals=number_digits)

    def round_date(self, x, frequency):
        #TODO: not yet implemented
        raise NotImplementedError
