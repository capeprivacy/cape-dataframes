from datetime import datetime

import numpy as np
from google.protobuf import timestamp_pb2

from .proto import data_connector_pb2
from .record import Record


def test_record():
    infos = [
        data_connector_pb2.FieldInfo(field=data_connector_pb2.BIGINT),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.INT),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.SMALLINT),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.DOUBLE),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.REAL),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.TIMESTAMP),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.TEXT),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.VARCHAR),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.CHAR),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.BOOL),
        data_connector_pb2.FieldInfo(field=data_connector_pb2.BYTEA),
    ]

    schema = data_connector_pb2.Schema(fields=infos)
    expected_vals = [
        4000,
        40,
        4,
        444.444,
        4.4,
        datetime(2016, 1, 1),
        "HEY",
        "WHATSUP",
        "TEST",
        False,
        bytes("notmuch", "ascii"),
    ]

    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(expected_vals[5])
    fields = [
        data_connector_pb2.Field(int64=expected_vals[0]),
        data_connector_pb2.Field(int32=expected_vals[1]),
        data_connector_pb2.Field(int32=expected_vals[2]),
        data_connector_pb2.Field(double=expected_vals[3]),
        data_connector_pb2.Field(float=expected_vals[4]),
        data_connector_pb2.Field(timestamp=ts),
        data_connector_pb2.Field(string=expected_vals[6]),
        data_connector_pb2.Field(string=expected_vals[7]),
        data_connector_pb2.Field(string=expected_vals[8]),
        data_connector_pb2.Field(bool=expected_vals[9]),
        data_connector_pb2.Field(bytes=expected_vals[10]),
    ]

    record = Record(schema, fields)

    i = 0
    for val in record.values:
        if type(val) is float:
            np.testing.assert_almost_equal(val, expected_vals[i])
        else:
            assert val == expected_vals[i]

        i += 1
