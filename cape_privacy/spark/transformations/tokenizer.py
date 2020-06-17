import hashlib
import uuid

from cape_privacy.spark import types
from cape_privacy.spark.transformations import base


class Tokenizer(base.Transformation):
    def __init__(self, key=None, **type_kwargs):
        super().__init__(input_type=types.String)
        self._key = key or uuid.uuid4().hex
        self._type_kwargs = type_kwargs

    def __call__(self, x):
        return self.to_token(x, **self._type_kwargs)

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


# def reverse_token(token, data, target_field, key):
#     # [NOTE] Currently requires to re-tokenize the dataset 
#     # to look-up the token, what would a more efficient way.
#     tokenizer = NativeTokenizer(key)
#     list_unique_elements = set(data[target_field])
#     token_dictionary = {tokenizer(el): el for el in list_unique_elements}
#     return token_dictionary[token]
