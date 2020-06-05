from typing import Any
from typing import Dict

import requests

from cape import connector
from cape.auth.api_token import APIToken
from cape.connector.stream import Stream
from cape.utils import base64


class GraphQLError:
    message: str
    extensions: Dict[str, Any]

    def __init__(self, error):
        self.message = error["message"]

        if "extensions" in error:
            self.extensions = error["extensions"]


class GraphQLException(Exception):
    def __init__(self, errors):
        self.errors = [GraphQLError(error) for error in errors]


# As the graphql spec is quite simple we're starting off here by writing
# the graphql queries directly as POST requests using the library requests.
class Client:
    def __init__(self, host: str, root_certificates: str = ""):
        self.root_certificates = root_certificates
        self.host = f"{host}/v1/query"
        self.token: str = ""

    def graphql_request(self, query: str, variables: Dict[str, str]):
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

    def pull(self, source: str, query: str, limit: int, offset: int) -> Stream:
        id = self.service_id_from_source(source)
        endpoint = self.service_endpoint(id)

        cl = connector.Client(
            endpoint, self.token, root_certificates=self.root_certificates
        )

        return cl.pull(source, query, limit, offset)
