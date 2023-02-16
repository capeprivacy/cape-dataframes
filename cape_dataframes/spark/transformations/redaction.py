from typing import List

from pyspark import sql


class ColumnRedact:
    """Redacts columns from a Spark dataframe.

    Attributes:
        columns: Which columns are redacted.
    """

    identifier = "column-redact"
    type_signature = "df->df"

    def __init__(self, columns: List[str]):
        self.columns = columns

    def __call__(self, df: sql.DataFrame) -> sql.DataFrame:
        return df.drop(*self.columns)


class RowRedact:
    """Redacts rows satisfying some condition from a Spark DataFrame.

    Attributes:
        condition: When this condition evaluates to True for a row, that row
            will be dropped.
    """

    identifier = "row-redact"
    type_signature = "df->df"

    def __init__(self, condition: str):
        self.condition = condition

    def __call__(self, df: sql.DataFrame) -> sql.DataFrame:
        cond = f"NOT {self.condition}"
        return df.filter(cond)
