import numpy as np
import pandas as pd

from cape_pandas import types
from cape_pandas.transformations import base

_INT_TO_DTYPE = {
    types.Byte: np.int8,
    types.Short: np.int16,
    types.Integer: np.int32,
    types.Long: np.int64,
}
_FREQUENCY_TO_TIMEDELTA = {
    'YEAR': lambda noise: pd.Timedelta(days=noise * 365),
    'MONTH': lambda noise: pd.Timedelta(days=noise * 30),
    'DAY': lambda noise: pd.Timedelta(days=noise),
    'HOUR': lambda noise: pd.Timedelta(hours=noise),
    'minutes': lambda noise: pd.Timedelta(minutes=noise),
    'seconds': lambda noise: pd.Timedelta(seconds=noise),
}


class Perturbation(base.Transformation):
    def __init__(self, input_type, **type_kwargs):
        super().__init__(input_type)
        self._type_kwargs = type_kwargs
        if self.type == types.Date:
            self._caller = self.add_noise_to_date
        elif self.type in (types.Float, types.Double):
            self._caller = self.add_noise_to_float
        elif self.type in (types.Byte, types.Short, types.Integer, types.Long):
            dtype = _INT_TO_DTYPE[self.type]
            self._caller = self.make_int_noise_caller(dtype)
        else:
            raise ValueError

    def __call__(self, x):
        return self._caller(x, **self._type_kwargs)

    def add_noise_to_float(self, x, low_boundary, high_boundary):
        noise = np.random.uniform(low_boundary, high_boundary)
        return x + noise

    def make_int_noise_caller(self, np_dtype):
        def add_noise_to_int(x, low_boundary, high_boundary):
            rng = np.random.default_rng()
            noise = rng.integers(low_boundary, high_boundary, dtype=np_dtype)
            return x + noise

        return add_noise_to_int

    def add_noise_to_date(self, date, frequency, low, high):
        # TODO manage random int dtype better?
        noise = np.random.randint(low, high)
        deltamaker = _FREQUENCY_TO_TIMEDELTA.get(frequency, None)
        if deltamaker is None:
            raise ValueError
        return date + deltamaker(noise)
