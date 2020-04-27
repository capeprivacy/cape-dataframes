from cape.utils import base64

from .private_key import derive_private_key


def test_private_key_sign():
    expected_sig = (
        "q6EpcYDaY3vls6H2muoz__7A7pzmu6PrcFwt-JtJSt"
        + "rM84XMa22wa0yv8ibcNWviCbqoW_sFnioVjehcVLmpCw"
    )
    salt = base64.from_string("SALTSALTSALTSALT")
    pkey = derive_private_key(b"my-strong-secret", salt)
    val = pkey.sign(base64.from_string("ABCDEFGHIJK"))

    assert expected_sig == str(val)
