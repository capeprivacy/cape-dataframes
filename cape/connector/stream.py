from typing import Iterator

import grpc
import pandas as pd
from google.protobuf import struct_pb2
from grpc_status import rpc_status

from .proto.data_connector_pb2 import Schema
from .record import Record


class Stream:
    def __init__(self, stream: Iterator):
        self.stream = stream
        self.schema: Schema = None

    def get_schema(self) -> Schema:
        return self.schema

    def __iter__(self):
        return self

    def __next__(self) -> Record:
        # raises stop iteration when done which will
        # then be caught by whatever is looping over this
        try:
            res = self.stream.__next__()
        except grpc.RpcError as rpc_error:
            status = rpc_status.from_call(rpc_error)
            for detail in status.details:
                if detail.Is(struct_pb2.Struct.DESCRIPTOR):
                    s = struct_pb2.Struct()

                    detail.Unpack(s)
                    msg = s["messages"].values[0].string_value
                    raise RuntimeError(f"Error while processing stream: {msg}")
                else:
                    raise RuntimeError("Unexpected failure: %s" % detail)

        if self.schema is None:
            self.schema = res.schema

        record = Record(self.schema, res.fields)

        return record

    def to_pandas(self) -> pd.DataFrame:
        df = None

        i = 0
        for record in self:
            if df is None:
                names = [field.name for field in self.schema.fields]
                df = pd.DataFrame(columns=names)

            df.loc[i] = record.values
            i += 1

        return df
