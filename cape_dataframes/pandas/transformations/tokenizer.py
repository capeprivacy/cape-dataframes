import hashlib
import secrets

import pandas as pd
from Crypto.Cipher import AES

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import base
from cape_privacy.utils import typecheck


class Tokenizer(base.Transformation):
    """Tokenizer: map a string to a token to obfuscate it.

    When applying the Tokenizer to a Pandas Series of type string,
    each value gets mapped to a token (hexadecimal string).
    If a value is repeated several times across the series, it always
    get mapped to the same token in order to maintain the count.
    A value can be mapped to different tokens by setting the key to a
    different value.

    Example:
        ```
        s = pd.Series(['A'])
        tokenize = Tokenizer(max_token_len=5, key='secret')
        tokenize(s) # pd.Series(['40a1e'])
        ```

    Attributes:
        max_token_len (int or bytes): control the token length (default
            length is 64)
        key: expect a string or byte string. If not specified, key will
            be set to a random byte string.
    """

    identifier = "tokenizer"
    type_signature = "col->col"

    def __init__(self, max_token_len: int = None, key: str = None):
        typecheck.check_arg(max_token_len, (int, type(None)))
        typecheck.check_arg(key, (str, bytes, type(None)))
        super().__init__(dtype=dtypes.String)
        self._max_token_len = max_token_len
        if isinstance(key, str):
            key = key.encode()
        self._key = key or secrets.token_bytes(8)

    def __call__(self, series: pd.Series) -> pd.Series:
        """Map a Pandas Series to tokens.

        Args:
            series (A Pandas Series): need to be a list of strings.

        Return:
            A Pandas Series with a list of tokens represented as hexadecimal
                strings.
        """

        return series.apply(lambda x: self.to_token(x))

    def to_token(self, x):
        token = hashlib.sha256(x.encode() + self.key).hexdigest()
        if self._max_token_len is not None:
            return token[: self._max_token_len]
        else:
            return token

    @property
    def key(self):
        return self._key


class ReversibleTokenizer(base.Transformation):
    """ReversibleTokenizer: map a string to a token to obfuscate it.

    When applying the Tokenizer to a Pandas Series of type string,
    each value gets mapped to a token (hexadecimal string).
    If a value is repeated several times across the series, it always
    get mapped to the same token in order to maintain the count.
    A value can be mapped to different tokens by setting the key to a
    different value.

    This tokenizer allows tokens to be reversed to their original data
    when the secret key is known.

    Example:
        ```
        s = pd.Series(['A'])
        tokenize = ReversibleTokenizer(key='secret')
        tokenize(s) # pd.Series(['40a1e'])
        ```

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

    def __call__(self, series: pd.Series) -> pd.Series:
        """Map a Pandas Series to tokens.

        Args:
            series (A Pandas Series): need to be a list of strings.

        Return:
            A Pandas Series with a list of tokens represented as hexadecimal
                strings.
        """

        return series.apply(self._to_token)

    def _to_token(self, x: str):
        cipher = AES.new(key=self.key, mode=AES.MODE_SIV)
        ciphertext, tag = cipher.encrypt_and_digest(x.encode(encoding=self.encoding))
        assert len(tag) == 16, len(tag)
        token = tag.hex() + ciphertext.hex()
        return token


class TokenReverser(base.Transformation):
    """TokenReverser: recover string from token.

    When applying the TokenReverser to a Pandas Series of tokens,
    each token is mapped back to the string that was originally used
    by ReversibleTokenizer to construct the token. The same key must
    be used.

    Example:
        ```
        s = pd.Series(['40a1e'])
        reverser = TokenReverser(key='secret')
        reverser(s) # pd.Series(['A'])
        ```

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

    def __call__(self, series: pd.Series) -> pd.Series:
        """Reverse a Pandas Series of tokens.

        Args:
            series (A Pandas Series): need to be a list of strings.

        Return:
            A Pandas Series with a list of recovered strings.
        """

        return series.apply(self._from_token)

    def _from_token(self, token: str):
        cipher = AES.new(key=self.key, mode=AES.MODE_SIV)
        token_bytes = bytearray.fromhex(token)
        tag, ciphertext = token_bytes[:16], token_bytes[16:]
        x = cipher.decrypt_and_verify(ciphertext, tag)
        return x.decode(encoding=self.encoding)
