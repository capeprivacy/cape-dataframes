from typing import Iterator

import pandas as pd

from .proto.data_connector_pb2 import Schema
from .record import Record


class Stream:
    def __init__(self, stream: Iterator):
        self.stream = stream
        self.schema: Schema = None

    def schema(self) -> Schema:
        return self.schema

    def __iter__(self):
        return self

    def __next__(self) -> Record:
        # raises stop iteration when done which will
        # then be caught by whatever is looping over this
        res = self.stream.__next__()

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
