from typing import Any
from typing import List

from .proto import data_connector_pb2


class Record:
    def __init__(
        self, schema: data_connector_pb2.Schema, fields: List[data_connector_pb2.Field]
    ):
        self.values = decode(schema, fields)


def decode(
    schema: data_connector_pb2.Schema, fields: List[data_connector_pb2.Field]
) -> List[Any]:
    arr: List[Any] = []

    i = 0
    for field in fields:
        field_type = schema.fields[i].field
        if field_type == data_connector_pb2.BIGINT:
            arr.append(field.int64)
        elif field_type == data_connector_pb2.INT:
            arr.append(field.int32)
        elif field_type == data_connector_pb2.SMALLINT:
            arr.append(field.int32)
        elif field_type == data_connector_pb2.TIMESTAMP:
            arr.append(field.timestamp.ToDatetime())
        elif field_type == data_connector_pb2.DOUBLE:
            arr.append(field.double)
        elif field_type == data_connector_pb2.REAL:
            arr.append(field.float)
        elif (
            field_type == data_connector_pb2.TEXT
            or field_type == data_connector_pb2.VARCHAR
            or field_type == data_connector_pb2.CHAR
        ):
            arr.append(field.string)
        elif field_type == data_connector_pb2.BOOL:
            arr.append(field.bool)
        elif field_type == data_connector_pb2.BYTEA:
            arr.append(field.bytes)

        i += 1

    return arr
