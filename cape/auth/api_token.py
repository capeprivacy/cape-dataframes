from cape.utils import base64

SECRET_BYTES = 16


class APIToken:
    email: str
    url: str
    version: int
    secret: bytes

    def __init__(self, token: str):
        splits = token.split(",")
        self.email = splits[0]

        token_bytes = bytes(base64.from_string(splits[1]))
        self.version = token_bytes[0]
        self.secret = token_bytes[1 : SECRET_BYTES + 1]
        self.url = token_bytes[SECRET_BYTES + 1 :].decode("utf-8")
