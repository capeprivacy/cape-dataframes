from .proto.data_connector_pb2 import BIGINT
from .proto.data_connector_pb2 import Field
from .proto.data_connector_pb2 import FieldInfo
from .proto.data_connector_pb2 import Record
from .proto.data_connector_pb2 import Schema
from .stream import Stream


class MockIterator:
    def __init__(self, num_entries: int):
        self.num_entries = num_entries
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        schema = None
        if self.num_entries == self.count:
            raise StopIteration

        if self.count == 0:
            info = [FieldInfo(name="BIGINT", field=BIGINT)]
            schema = Schema(fields=info)

        record = Record(schema=schema, fields=[Field(int64=4000)])

        self.count += 1

        return record


def test_stream():
    iterations = 10
    it = MockIterator(iterations)

    stream = Stream(it)

    records = list(stream)
    assert len(records) == iterations

    for record in records:
        assert record.values[0] == 4000


def test_to_pandas():
    iterations = 10
    it = MockIterator(iterations)

    stream = Stream(it)

    records = stream.to_pandas()

    assert len(records) == iterations

    for _, record in records.iterrows():
        assert record["BIGINT"] == 4000
