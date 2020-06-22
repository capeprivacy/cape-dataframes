import pandas as pd


class RowRedact:
    """Redacts rows based on the condition.

    Attributes:
        condition: The condition to be passed into the query function.
    """

    identifier = "row-redact"
    type_signature = "df->df"

    def __init__(self, condition: str) -> None:
        self.condition = condition

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """Redacts rows using Dataframe.query.

        DataFrame.query returns all the fields that it matches so
        we negate it here to get the opposite.
        """

        condition = f"~({self.condition})"
        return df.query(condition)
