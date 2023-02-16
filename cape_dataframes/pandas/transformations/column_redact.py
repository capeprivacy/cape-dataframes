from typing import List

import pandas as pd


class ColumnRedact:
    """Redacts columns.

    Attributes:
        columns: The columns to redact.
    """

    identifier = "column-redact"
    type_signature = "df->df"

    def __init__(self, columns: List[str]) -> None:
        self.columns = columns

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=self.columns)
