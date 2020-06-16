import numpy as np
import pandas as pd

from cape_spark.transformations import base


class Perturbation(base.Transformation): 
    def __init__(self, input_type, **type_kwargs):
        super().__init__(input_type)
        self._type_kwargs = type_kwargs
        if self.type == 'date':
            self._caller = self.add_noise_to_date
        elif self.type == 'float':
            self._caller = self.add_noise_to_numeric
        else:
            raise ValueError

    def __call__(self, x):
        return self._caller(x, **self._type_kwargs)

    def add_noise_to_numeric(self, x, low_boundary, high_boundary):
        noise = np.random.uniform(low_boundary, high_boundary)
        return x + noise

    def add_noise_to_date(self, date, frequency, low, high):
        noise = np.random.randint(low, high)
        if frequency == 'YEAR':
            return date + pd.timedelta(days=noise*365)
        elif frequency == 'MONTH':
            return date + pd.timedelta(days=noise*30)
        elif frequency == 'DAY':
            return date + pd.timedelta(days=noise)
        elif frequency == 'HOUR':
            return date + pd.timedelta(hours=noise)
        elif frequency == 'minutes':
            return date + pd.timedelta(minutes=noise)
        elif frequency == 'seconds':
            return date + pd.timedelta(seconds=noise)
        else:
            raise ValueError
