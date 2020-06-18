from typing import Any
from typing import Dict

import requests

from cape_privacy.auth.api_token import APIToken
from cape_privacy.utils import base64


class GraphQLError:
    """Represents a graphql error that can be returned by a coordinator.

    Attributes:
        message: The error message.
        extensions: Any extra information returned by coordinator.
    """

    message: str
    extensions: Dict[str, Any]

    def __init__(self, error):
        self.message = error["message"]

        if "extensions" in error:
            self.extensions = error["extensions"]


class GraphQLException(Exception):
    """Exception wrapping a list of GraphQL errors.

    Attributes:
        errors: List of GraphQL errors.
    """

    def __init__(self, errors):
        self.errors = [GraphQLError(error) for error in errors]


class Client:
    """Coordinator client for making GraphQL requests.

    Implements a simple GraphQL protocol to communicate with a
    coordinator.

    Attributes:
        host: The address of the coordinator.
        token: The token used to authenticate with a coordinator.
    """

    def __init__(self, host: str):
        self.host = f"{host}/v1/query"
        self.token: str = ""

    def graphql_request(self, query: str, variables: Dict[str, str]):
        """Makes a GraphQL request to a coordinator.

        Adds an authorization header if it exists.

        Arguments:
            query: The GraphQL query to be passed to a coordinator.
            variables: The variables to be passed to a coordinator.

        Returns:
            The coordinator's GraphQL data response.

        Raises:
            GraphQLException: If a GraphQL error occurs.
        """

        headers = {}
        if self.token != "":
            headers["Authorization"] = f"Bearer {self.token}"

        r = requests.post(
            self.host, headers=headers, json={"query": query, "variables": variables},
        )

        # attempt to get json so we can get the errors
        # if an error has occurred, if json doesn't exist
        # just raise the error
        try:
            j = r.json()
        except ValueError:
            r.raise_for_status()

        if "errors" in j:
            raise GraphQLException(j["errors"])

        return j["data"]

    def service_id_from_source(self, label: str):
        """Gets the services from the given source label."""

        query = """
        query SourceQuery($label: Label!) {
            sourceByLabel(label: $label) {
                service {
                    id
                }
            }
        }
        """

        variables = {"label": label}

        res = self.graphql_request(query, variables)

        return res["sourceByLabel"]["service"]["id"]

    def service_endpoint(self, id):
        """Gets the service endpoint for the given id"""

        query = """
        query Service($id: ID!) {
            service(id: $id) {
                endpoint
            }
        }
        """

        variables = {"id": id}

        res = self.graphql_request(query, variables)

        return res["service"]["endpoint"]

    def login(self, token: str):
        """Logs in with the given token string"""

        api_token = APIToken(token)

        query = """
        mutation CreateSession($token_id: ID, $secret: Password!) {
            createSession(input: { token_id: $token_id, secret: $secret }) {
                token
            }
        }
        """

        variables = {
            "token_id": api_token.token_id,
            "secret": str(base64.Base64(api_token.secret)),
        }

        res = self.graphql_request(query, variables)

        self.token = base64.from_string(res["createSession"]["token"])

        return self.token

    def identity_policies(self, id: str) -> [Dict[Any, Any]]:
        """Returns all policies for the given identity id."""

        query = """
        query IdentityPolicies($id: ID!) {
            identityPolicies(identity_id: $id) {
                spec
            }
        }
        """

        variables = {
            "id": id,
        }

        res = self.graphql_request(query, variables)

        return res["identityPolicies"]

    def me(self) -> str:
        """Returns the id of the authenticated identity."""

        query = """
        query Me() {
            me {
                id
            }
        }
        """

        res = self.graphql_request(query, None)

        return res["me"]["id"]

    def query_policies(self) -> [Dict[Any, Any]]:
        """Queries all of the policies for the authenticated identity."""

        id = self.me()

        return self.identity_policies(id)
