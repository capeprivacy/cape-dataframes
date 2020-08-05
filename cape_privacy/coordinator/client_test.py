import json

import pytest
import responses

from cape_privacy.coordinator.auth.api_token import create_api_token
from cape_privacy.coordinator.client import Client
from cape_privacy.coordinator.client import GraphQLException

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
        c.me()

    g_err = excinfo.value.errors[0]
    assert g_err.message == "Access denied"
    assert g_err.extensions == {
        "cause": {"name": "authorization_failure", "category": "unauthorized"}
    }


@responses.activate
def test_login():
    exp_token = "ABCDEFE"
    token_id = "specialid"
    secret = "secret"

    token = create_api_token(token_id, secret)

    def cb(request):
        resp_body = {"token": exp_token}

        return 200, {}, json.dumps(resp_body)

    responses.add_callback(responses.POST, f"{host}/v1/login", cb)

    c = Client(host)

    c.login(token.raw)

    assert str(c.token) == exp_token


@responses.activate
def test_me():
    my_id = "thisisanid"
    responses.add(
        responses.POST, f"{host}/v1/query", json={"data": {"me": {"id": my_id}}},
    )

    c = Client(host)

    id = c.me()

    assert my_id == id


@responses.activate
def test_get_policy():
    rules = [{"match": "column", "actions": [{"transform": {"name": "plusOne"}}]}]

    responses.add(
        responses.POST,
        f"{host}/v1/query",
        json={"data": {"project": {"current_spec": {"rules": rules}}}},
    )

    c = Client(host)

    policy = c.get_policy("random-project")

    expected = {"label": "random-project", "rules": rules}

    assert policy == expected


def test_client_repr():
    c = Client(host)

    assert c.__repr__() == f"This client is connected to {host}"
