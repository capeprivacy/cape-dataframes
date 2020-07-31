import hashlib
import secrets

import pandas as pd
from Crypto.Cipher import AES
from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import base
from cape_privacy.utils import typecheck


class Tokenizer(base.Transformation):
    """Tokenizer: map a string to a token to obfuscate it.

    When applying the tokenizer to a Spark series of type string,
    each value gets mapped to a token (hexadecimal string).
    If a value is repeated several times across the series, it always
    get mapped to the same token in order to maintain the count.
    A value can be mapped to different tokens by setting the key to a
    different value.

    Attributes:
        max_token_len (int or bytes): control the token length (default
            length is 64)
        key: expect a string or byte string. if not specified, key will
            be set to a random byte string.
    """

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


class ReversibleTokenizer(base.Transformation):
    """ReversibleTokenizer: map a string to a token to obfuscate it.

    When applying the Tokenizer to a Spark series of type string,
    each value gets mapped to a token (hexadecimal string).
    If a value is repeated several times across the series, it always
    get mapped to the same token in order to maintain the count.
    A value can be mapped to different tokens by setting the key to a
    different value.

    This tokenizer allows tokens to be reversed to their original data
    when the secret key is known.

    Attributes:
        key: expect a string or byte string of length exactly 32 bytes.
        encoding: string identifying the Python encoding used for inputs.
    """

    identifier = "reversible-tokenizer"
    type_signature = "col->col"

    def __init__(self, key, encoding="utf-8"):
        typecheck.check_arg(key, (str, bytes))
        typecheck.check_arg(encoding, str)
        super().__init__(dtype=dtypes.String)
        if isinstance(key, str):
            key = key.encode()
        if len(key) != 32:
            raise ValueError(f"Key must be exactly 32 bytes, got {len(key)}")
        self.key = key
        self.encoding = encoding

    def __call__(self, series):
        @functions.pandas_udf(dtypes.String, functions.PandasUDFType.SCALAR)
        def to_token(series):
            return series.map(self._to_token)

        return to_token(series)

    def _to_token(self, x: str):
        cipher = AES.new(key=self.key, mode=AES.MODE_SIV)
        ciphertext, tag = cipher.encrypt_and_digest(x.encode(encoding=self.encoding))
        assert len(tag) == 16, len(tag)
        token = tag.hex() + ciphertext.hex()
        return token


class TokenReverser(base.Transformation):
    """TokenReverser: recover string from token.

    When applying the TokenReverser to a Spark series of tokens,
    each token is mapped back to the string that was originally used
    by ReversibleTokenizer to construct the token. The same key must
    be used.

    Attributes:
        key: expect a string or byte string of length exactly 32 bytes.
        encoding: string identifying the Python encoding used for outputs.
    """

    identifier = "token-reverser"
    type_signature = "col->col"

    def __init__(self, key, encoding="utf-8"):
        typecheck.check_arg(key, (str, bytes))
        typecheck.check_arg(encoding, str)
        super().__init__(dtype=dtypes.String)
        if isinstance(key, str):
            key = key.encode()
        if len(key) != 32:
            raise ValueError(f"Key must be exactly 32 bytes, got {len(key)}")
        self.key = key
        self.encoding = encoding

    def __call__(self, series) -> pd.Series:
        @functions.pandas_udf(dtypes.String, functions.PandasUDFType.SCALAR)
        def from_token(series):
            return series.map(self._from_token)

        return from_token(series)

    def _from_token(self, token: str):
        cipher = AES.new(key=self.key, mode=AES.MODE_SIV)
        token_bytes = bytearray.fromhex(token)
        tag, ciphertext = token_bytes[:16], token_bytes[16:]
        x = cipher.decrypt_and_verify(ciphertext, tag)
        return x.decode(encoding=self.encoding)
