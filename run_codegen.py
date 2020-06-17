import os

import grpc_tools
from grpc_tools import protoc

path = os.path.dirname(grpc_tools.__file__)
path = path + "/_proto/"

protoc.main((
    '',
    '-I../',
    '-I' + path,
    '--python_out=.',
    '--grpc_python_out=.',
    '../pandas/connector/proto/data_connector.proto',
))
