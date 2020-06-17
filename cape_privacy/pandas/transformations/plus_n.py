import pandas as pd


class PlusN:
    """A sample transform that adds n to a specific field.

    Attributes:
        field: The field that this transform will be applied to.
        n: The value to add to the field.
    """

    function = "plusN"

    def __init__(self, field: str, n: int = 1) -> None:
        self.n = n
        self.field = field

    def __call__(self, column: pd.Series) -> pd.Series:
        return column + self.n
