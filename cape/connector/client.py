import grpc

from cape.utils import base64

from .proto import data_connector_pb2
from .proto.data_connector_pb2_grpc import DataConnectorStub
from .stream import Stream


class Client:
    stub: DataConnectorStub
    token: base64.Base64

    def __init__(self, host: str, token: base64.Base64, root_certificates=""):
        creds = grpc.ssl_channel_credentials()
        if root_certificates != "":
            with open(root_certificates, "br") as f:
                cert = f.read()

            creds = grpc.ssl_channel_credentials(root_certificates=cert)

        self.token = token

        host = host.strip("https://")
        self.channel = grpc.secure_channel(host, creds)
        self.stub = DataConnectorStub(self.channel)

    def pull(self, source: str, query: str, limit: int, offset: int) -> Stream:
        req = data_connector_pb2.QueryRequest(
            data_source=source, query=query, limit=limit, offset=offset
        )

        call_credentials = grpc.access_token_call_credentials(str(self.token))

        return Stream(self.stub.Query(request=req, credentials=call_credentials))

    def close(self):
        self.channel.close()
