from cape.utils import base64


class Credentials:
    def __init__(self, salt: base64.Base64, alg: str):
        self.salt = salt
        self.alg = alg

    def __eq__(self, other):
        return str(self.salt) == str(other.salt) and self.alg == other.alg
