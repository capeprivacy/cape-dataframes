import json

import pytest
import responses

from cape.auth.api_token import create_api_token

from .client import Client
from .client import GraphQLException

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
        json={"data": {"sourceByLabel": {"service": {"id": service_id}}}},
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
        json={"data": {"sourceByLabel": {"service": {"id": service_id}}}},
    )

    c = Client(host)

    id = c.service_id_from_source("label")

    assert service_id == id


@responses.activate
def test_login():
    exp_token = "ABCDEFE"
    token_id = "specialid"
    secret = "secret"

    token = create_api_token(token_id, secret)

    def cb(request):
        resp_body = {"data": {"createSession": {"token": exp_token}}}

        return 200, {}, json.dumps(resp_body)

    responses.add_callback(responses.POST, f"{host}/v1/query", cb)

    c = Client(host)

    c.login(token.raw)

    assert str(c.token) == exp_token
