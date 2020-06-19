import hashlib
import secrets

import pandas as pd
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base
from cape_privacy.utils import typecheck


class Tokenizer(base.Transformation):
    def __init__(self, max_token_len=None, key=None):
        typecheck.check_arg(max_token_len, (int, type(None)))
        typecheck.check_arg(key, (str, bytes, type(None)))
        super().__init__(input_type=dtypes.String)
        self._max_token_len = max_token_len
        if isinstance(key, str):
            key = key.encode()
        self._key = key or secrets.token_bytes(8)

    def __call__(self, x):
        return self._to_token_udf(x)

    @functions.pandas_udf(dtypes.String, functions.PandasUDFType.SCALAR)
    def _to_token_udf(self, x: pd.Series):
        return x.map(self.to_token)

    def to_token(self, x: str):
        token = hashlib.sha256(x.encode() + self.key).hexdigest()
        if self._max_token_len is None:
            return token
        return token[:self._max_token_len]

    @property
    def key(self):
        return self._key
