from typing import List

import pandas as pd


class ColumnRedact:
    """Redacts columns.

    Attributes:
        columns: The columns to redact.
    """

    function = "column_redact"

    def __init__(self, columns: List[str]) -> None:
        self.columns = columns

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=self.columns)
