import pytest
import responses

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
    exp_token = "ABCDEFGH"
    exp_creds = Credentials("SALTSALTSALTSALT", "EDDSA")

    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={
            "data": {
                "createLoginSession": {
                    "token": exp_token,
                    "credentials": {"salt": exp_creds.salt, "alg": exp_creds.alg},
                }
            }
        },
    )

    c = Client(host)

    token, creds = c.create_login_session("justin@capeprivacy.com")

    assert token == bytes(exp_token, "ascii")
    assert creds == exp_creds


@responses.activate
def test_create_auth_session():
    exp_token = "ABCDEFG"

    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={"data": {"createAuthSession": {"token": exp_token}}},
    )

    c = Client(host)

    token = c.create_auth_session(b"ABCDEFG")

    assert token == exp_token
