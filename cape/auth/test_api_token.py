from .api_token import APIToken


def test_api_token():
    token = "hey@hey.com,AWa4m7eD3ki8lg-rhuTHT-VodHRwOi8vbG9jYWxob3N0OjgwODA"

    a_token = APIToken(token)

    assert a_token.secret == b"f\xb8\x9b\xb7\x83\xdeH\xbc\x96\x0f\xab\x86\xe4\xc7O\xe5"
    assert a_token.url == "http://localhost:8080"
    assert a_token.version == 1
