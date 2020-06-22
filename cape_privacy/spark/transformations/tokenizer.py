import hashlib
import secrets

import pandas as pd
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base
from cape_privacy.utils import typecheck


class Tokenizer(base.Transformation):
    identifier = "tokenizer"
    type_signature = "col->col"

    def __init__(self, max_token_len=None, key=None):
        typecheck.check_arg(max_token_len, (int, type(None)))
        typecheck.check_arg(key, (str, bytes, type(None)))
        super().__init__(dtypes.String)
        self._max_token_len = max_token_len
        if isinstance(key, str):
            key = key.encode()
        self._key = key or secrets.token_bytes(8)
        self._tokenize = None

    def __call__(self, x):
        if self._tokenize is None:
            self._tokenize = self._make_tokenize_udf()
        return self._tokenize(x)

    def _make_tokenize_udf(self):
        @functions.pandas_udf(dtypes.String, functions.PandasUDFType.SCALAR)
        def to_token(x: pd.Series):
            return x.map(self._to_token)

        return to_token

    def _to_token(self, x: str):
        token = hashlib.sha256(x.encode() + self.key).hexdigest()
        if self._max_token_len is None:
            return token
        return token[: self._max_token_len]

    @property
    def key(self):
        return self._key
