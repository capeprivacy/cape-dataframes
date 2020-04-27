from typing import Any
from typing import Dict

import requests

from cape.auth import derive_private_key
from cape.utils import base64

from .credentails import Credentials


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
    def __init__(self, host: str):
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
                service_id
            }
        }
        """

        variables = {"label": label}

        res = self.graphql_request(query, variables)

        return res["sourceByLabel"]["service_id"]

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

    def create_login_session(self, email: str) -> (base64.Base64, Credentials):
        query = """
        mutation CreateLoginSession($email: Email!) {
            createLoginSession(input: { email: $email }) {
                token
                credentials {
                    salt
                    alg
                }
            }
        }
        """

        variables = {"email": email}

        res = self.graphql_request(query, variables)

        token = base64.from_string(res["createLoginSession"]["token"])
        salt = base64.from_string(res["createLoginSession"]["credentials"]["salt"])
        alg = res["createLoginSession"]["credentials"]["alg"]

        return token, Credentials(salt, alg)

    def create_auth_session(self, signature: base64.Base64) -> base64.Base64:
        query = """
        mutation CreateAuthSession($signature: Base64!) {
            createAuthSession(input: { signature: $signature }) {
                token
            }
        }
        """

        variables = {"signature": str(signature)}

        res = self.graphql_request(query, variables)

        return base64.from_string(res["createAuthSession"]["token"])

    def login(self, email: str, secret: bytes):
        token, creds = self.create_login_session(email)

        self.token = token

        pkey = derive_private_key(secret, creds.salt)

        sig = pkey.sign(token)

        self.token = self.create_auth_session(sig)
