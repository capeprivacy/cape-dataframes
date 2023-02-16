import pandas as pd


class PlusN:
    """A sample transform that adds n to a specific field.

    Attributes:
        field: The field that this transform will be applied to.
        n: The value to add to the field.
    """

    identifier = "plusN"
    type_signature = "col->col"

    def __init__(self, n: int = 1) -> None:
        self.n = n

    def __call__(self, column: pd.Series) -> pd.Series:
        return column + self.n
