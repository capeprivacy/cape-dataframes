from .client import Client
from .stream_test import MockIterator


class MockStub:
    def __init__(self, iterations):
        self.iterations = iterations

    def Query(self, request):
        return MockIterator(self.iterations)


def test_client(mocker):
    iterations = 10

    client = Client("localhost:8081")

    # override here so it doesn't actually do networking
    client.stub = MockStub(iterations)

    stream = client.pull("creditcards", "SELECT * FROM transactions", 10, 0)
    df = stream.to_pandas()

    assert len(df) == iterations

    for _, record in df.iterrows():
        assert record["BIGINT"] == 4000
