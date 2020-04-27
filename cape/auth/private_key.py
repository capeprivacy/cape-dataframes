from hashlib import scrypt

from nacl.signing import SigningKey

from cape.utils import base64

N = 32768
R = 8
P = 1
SEED_SIZE = 32


class PrivateKey:
    private_key: SigningKey

    def __init__(self, private_key: SigningKey):
        self.private_key = private_key

    def sign(self, token: base64.Base64) -> base64:
        s_token = self.private_key.sign(bytes(token))

        return base64.Base64(s_token.signature)


def derive_private_key(secret: bytes, salt: base64.Base64) -> PrivateKey:
    key = scrypt(
        secret, salt=bytes(salt), n=N, r=R, p=P, maxmem=64000000, dklen=SEED_SIZE
    )

    private_key = SigningKey(key)

    return PrivateKey(private_key)
