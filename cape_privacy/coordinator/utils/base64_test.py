from cape_privacy.coordinator.utils.base64 import Base64
from cape_privacy.coordinator.utils.base64 import from_string


def test_base64():
    b64 = Base64("heythere")
    assert "aGV5dGhlcmU" == str(b64)


def test_from_string():
    s = "ABCD"
    b64 = from_string(s)

    assert s == str(b64)
