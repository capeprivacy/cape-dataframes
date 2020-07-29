from base64 import urlsafe_b64decode
from base64 import urlsafe_b64encode
from typing import Union


# This implements a similar wrapped as cape has in golang.
# It stores the bytes and converts it to encoded string as needed.
# The python base64 package appends padding when encoding but
# in cape this causes errors to occur so must strip that off
# before sending.
class Base64:
    def __init__(self, value: Union[str, bytes]):
        self.bytes = value
        if type(value) == str:
            self.bytes = bytes(value, "utf-8")

    def __bytes__(self) -> bytes:
        return self.bytes

    # returns the base64 encoded value as a string
    def __str__(self) -> str:
        b = urlsafe_b64encode(self.bytes)
        b = b.strip(b"==")

        return str(b, "utf-8")


# Returns a Base64 object from the base64 encoded string.
# Adds padding when decoding so that it doesn't error.
def from_string(s: str) -> Base64:
    b = urlsafe_b64decode(bytes(s, "utf-8") + b"==")
    return Base64(b)
