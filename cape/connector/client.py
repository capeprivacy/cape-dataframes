import grpc

from .proto import data_connector_pb2
from .proto.data_connector_pb2_grpc import DataConnectorStub
from .stream import Stream


class Client:
    stub: DataConnectorStub

    def __init__(self, host: str, root_certificates=""):
        creds = grpc.ssl_channel_credentials()
        if root_certificates != "":
            with open(root_certificates, "br") as f:
                cert = f.read()

            creds = grpc.ssl_channel_credentials(root_certificates=cert)

        self.channel = grpc.secure_channel(host, creds)
        self.stub = DataConnectorStub(self.channel)

    def pull(self, source: str, query: str, limit: int, offset: int) -> Stream:
        req = data_connector_pb2.QueryRequest(
            data_source=source, query=query, limit=limit, offset=offset
        )

        return Stream(self.stub.Query(request=req))

    def close(self):
        self.channel.close()
