import hashlib
import uuid

import pandas as pd
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base


class Tokenizer(base.Transformation):
    def __init__(self, token_len, key=None):
        super().__init__(input_type=dtypes.String)
        self._key = key or uuid.uuid4().hex
        self._type_kwargs = type_kwargs

    def __call__(self, x):
        return self._to_token_udf(x)

    @functions.pandas_udf(dtypes.String, functions.PandasUDFType.SCALAR)
    def _to_token_udf(self, x: pd.Series):
        return x.map(self.to_token)

    def to_token(self, x, size=None):
        token = hashlib.sha256(x.encode() + self.key.encode()).hexdigest() 
        if size is not None:
            return token[:size]
        else:
            return token

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, new_key):
        self._key = new_key
