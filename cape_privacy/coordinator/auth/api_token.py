from cape_privacy.coordinator.utils import base64

SECRET_BYTES = 16
VERSION = b"\x01"


class APIToken:
    """Represents an API token used to authenticate with the coordinator.

    The format is: <token_id>,<base64 string>

    The first byte of the decoded Base64 string is the version and the rest
    is the secret.

    Attributes:
        token_id: The ID of the token.
        version: The version of the token format.
        secret: The password used to authenticate.
        raw: The raw token string.
    """

    token_id: str
    version: bytes
    secret: bytes
    raw: str

    def __init__(self, token: str):
        self.raw = token
        splits = token.split(",")
        self.token_id = splits[0]

        token_bytes = bytes(base64.from_string(splits[1]))
        self.version = token_bytes[0]
        self.secret = token_bytes[1:]


def create_api_token(token_id: str, secret: bytes) -> APIToken:
    """Creates an APIToken. Mostly used for testing.

    Args:
        token_id: The token id to use.
        secret: The password to use.

    Returns:
        The constructed APIToken.
    """
    token_bytes = bytes(VERSION) + bytes(secret, "utf-8")
    b64 = base64.Base64(token_bytes)

    token = f"{token_id},{b64}"

    return APIToken(token)
