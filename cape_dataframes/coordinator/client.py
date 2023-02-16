from datetime import datetime
from typing import Any
from typing import Dict

import requests
import rfc3339

from cape_dataframes.coordinator.auth.api_token import APIToken
from cape_dataframes.policy import parse_policy
from cape_dataframes.policy.data import Policy
from cape_dataframes.utils import base64


class GraphQLError:
    """Represents a GraphQL error that can be returned by a coordinator.

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


class CapeError:
    """Represents a Cape error coming from the coordinator.

    Attributes:
        messages: A list of error messages
        cause: The cause of the error
    """

    def __init__(self, error):
        self.messages = error["messages"]
        self.cause = error["cause"]


class CapeException(Exception):
    """Exception wrapping a CapeError.
    Attributes:
        error: the CapeError
    """

    def __init__(self, error):
        self.error = error


class Client:
    """Coordinator client for making GraphQL requests.

    Implements a simple GraphQL protocol to communicate with a
    coordinator.

    Attributes:
        host: The address of the coordinator.
        token: The token used to authenticate with a coordinator.
    """

    def __init__(self, host: str):
        self.host = f"{host}"
        self.token: str = ""

        self.s = requests.Session()

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

        r = self.s.post(
            f"{self.host}/v1/query",
            json={"query": query, "variables": variables},
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

    def login(self, token: str):
        """Logs in with the given token string"""

        self.api_token = APIToken(token)

        r = self.s.post(
            f"{self.host}/v1/login",
            json={
                "token_id": self.api_token.token_id,
                "secret": str(base64.Base64(self.api_token.secret)),
            },
        )

        # attempt to get json so we can get the errors
        # if an error has occurred, if json doesn't exist
        # just raise the error
        try:
            j = r.json()
        except ValueError:
            r.raise_for_status()

        if "cause" in j:
            raise CapeException(j)

        self.token = base64.from_string(j["token"])

        self.user = self.me()

        return self.token

    def me(self) -> str:
        """Returns the ID of the authenticated identity."""

        query = """
        query Me() {
            me {
                id
                name
                email
            }
        }
        """

        res = self.graphql_request(query, None)

        return res["me"]

    def get_policy(self, label: str) -> Policy:
        """Returns the current policy for a given project label."""

        query = """
        query CurrentSpec($label: ModelLabel!) {
            project(label: $label) {
                current_spec {
                    id
                    rules
                    transformations
                }
            }
        }
        """

        variables = {
            "label": label,
        }

        res = self.graphql_request(query, variables)

        spec = res["project"]["current_spec"]
        spec["label"] = label

        return parse_policy(spec, logger=self)

    def audit_log(self, event_name, target_id, target_type, target_label):
        """Returns the current policy for a given project label."""

        query = """
        mutation AddAuditLog($audit: AuditEventInput!) {
            addAuditLog(audit: $audit) {
                event_name
            }
        }
        """

        variables = {
            "audit": {
                "event_name": event_name,
                "user_id": self.user["id"],
                "user_name": self.user["name"],
                "user_email": self.user["email"],
                "time": rfc3339.rfc3339(datetime.now()),
                "target_id": target_id,
                "target_type": target_type,
                "target_label": target_label,
            },
        }

        self.graphql_request(query, variables)

    def __repr__(self):
        return f"This client is connected to {self.host}"
