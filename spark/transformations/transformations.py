import datetime
import hashlib
import uuid

import numpy as np
import pandas as pd


class Tokenizer:
    def __init__(self, salt=None):
        self.salt = salt or uuid.uuid4().hex

    def __call__(self, x, size=None):
        return self.to_token(x, size)
    
    def to_token(self, x, size=None):  
        token = hashlib.sha256(x.encode() + self.salt.encode()).hexdigest()

        if size is not None:
            return token[:size]
        else:
            return token

    def update_salt(self, salt):
        self.salt = salt


def reverse_token(token, data, target_field, salt):
    # [NOTE] Currently requires to re-tokenize the dataset 
    # to look-up the token, what would a more efficient way.
    tokenizer = Tokenizer(salt)
    list_unique_elements = set(data[target_field])
    token_dictionary = {tokenizer.to_token(el): el for el in list_unique_elements}
    return token_dictionary[token]


class Rounding:
    def round_numeric(self, x, number_digits):
        return round(x, number_digits)
    
    def round_date(self, x, frequency):
        # [NOTE] should be reviewed to match a SQL round
        # https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions136.htm
        if frequency=='YEAR':
            return datetime.date(x.year, 1, 1)
        elif frequency == 'MONTH':
            return datetime.date(x.year, x.month, 1)
        else:
            raise ValueError


class Perturbation: 
    def add_noise_to_numeric(self, x, low_boundary, high_boundary):
        noise = np.random.uniform(low_boundary, high_boundary)
        if isinstance(x, int):
            return int(x + noise)
        else:
            return float(x + noise)

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
