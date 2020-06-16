import hashlib
import uuid

from transformations import base


class NativeTokenizer(base.Transformation):
    def __init__(self, salt=None):
        super().__init__(type='string')
        self._salt = salt or uuid.uuid4().hex

    def __call__(self, x, size=None):
        return self.to_token(x, size)

    def to_token(self, x, size=None):  
        token = hashlib.sha256(x.encode() + self.salt.encode()).hexdigest() 

        if size is not None:
            return token[:size]
        else:
            return token

    @property
    def salt(self):
      return self._salt

    @salt.setter
    def salt(self, value):
      self._salt = value


def reverse_token(token, data, target_field, salt):
    # [NOTE] Currently requires to re-tokenize the dataset 
    # to look-up the token, what would a more efficient way.
    tokenizer = NativeTokenizer(salt)
    list_unique_elements = set(data[target_field])
    token_dictionary = {tokenizer(el): el for el in list_unique_elements}
    return token_dictionary[token]
