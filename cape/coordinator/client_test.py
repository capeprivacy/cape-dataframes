import json

import pytest
import responses

from cape.auth import derive_private_key
from cape.utils import base64

from .client import Client
from .client import GraphQLException
from .credentails import Credentials

host = "http://localhost:8080"


@responses.activate
def test_graphql_error():
    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={
            "errors": [
                {
                    "message": "Access denied",
                    "extensions": {
                        "cause": {
                            "name": "authorization_failure",
                            "category": "unauthorized",
                        }
                    },
                }
            ]
        },
    )

    c = Client(host)

    with pytest.raises(GraphQLException) as excinfo:
        c.service_id_from_source("test")

    g_err = excinfo.value.errors[0]
    assert g_err.message == "Access denied"
    assert g_err.extensions == {
        "cause": {"name": "authorization_failure", "category": "unauthorized"}
    }


@responses.activate
def test_service_id_from_source():
    service_id = "thisisanid"
    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={"data": {"sourceByLabel": {"service_id": service_id}}},
    )

    c = Client(host)

    id = c.service_id_from_source("label")
    assert service_id == id


@responses.activate
def test_service_endpoint():
    service_id = "thisisanid"
    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={"data": {"sourceByLabel": {"service_id": service_id}}},
    )

    c = Client(host)

    id = c.service_id_from_source("label")

    assert service_id == id


@responses.activate
def test_create_login_session():
    exp_token = "ABCDEFE"
    exp_creds = Credentials(base64.from_string("SALTSALTSALTSALT"), "EDDSA")

    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={
            "data": {
                "createLoginSession": {
                    "token": exp_token,
                    "credentials": {"salt": str(exp_creds.salt), "alg": exp_creds.alg},
                }
            }
        },
    )

    c = Client(host)

    token, creds = c.create_login_session("justin@capeprivacy.com")

    assert str(token) == exp_token
    assert creds == exp_creds


@responses.activate
def test_create_auth_session():
    exp_token = "ABCDEFE"

    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={"data": {"createAuthSession": {"token": exp_token}}},
    )

    c = Client(host)

    token = c.create_auth_session(b"ABCDEFG")

    assert str(token) == exp_token


@responses.activate
def test_login():
    exp_login_token = "ABCDEFE"
    exp_session_token = "ABCDEFE"
    email = "hey@hey.com"
    password = b"whatsupcape"
    salt = "SALTSALTSALTSALT"

    pkey = derive_private_key(password, base64.from_string(salt))
    pub_key = pkey.private_key.verify_key

    def cb(request):
        payload = json.loads(request.body)
        query = payload["query"]
        variables = payload["variables"]

        resp_body = {}
        if "createLoginSession" in query:
            resp_body = {
                "data": {
                    "createLoginSession": {
                        "token": exp_login_token,
                        "credentials": {"salt": salt, "alg": "EDDSA"},
                    }
                }
            }
        elif "createAuthSession" in query:
            sig = base64.from_string(variables["signature"])
            msg = base64.from_string(exp_login_token)

            pub_key.verify(bytes(msg), bytes(sig))

            resp_body = {"data": {"createAuthSession": {"token": exp_session_token}}}

        return 200, {}, json.dumps(resp_body)

    responses.add_callback(responses.POST, f"{host}/v1/query", cb)

    c = Client(host)

    c.login(email, password)

    assert str(c.token) == exp_session_token
