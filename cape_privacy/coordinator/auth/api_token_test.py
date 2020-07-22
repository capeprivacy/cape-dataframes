from cape_privacy.coordinator.auth.api_token import create_api_token


def test_api_token():
    token_id = "imatokenid"
    secret = "aaaabbbbccccdddd"
    token = create_api_token(token_id, secret)

    assert token.token_id == token_id
    assert token.secret == bytes(secret, "utf-8")
    assert token.version == 1
