import pandas as pd


class PlusN:
    function = "plusN"

    def __init__(self, field: str, n: int = 1) -> None:
        self.n = n
        self.field = field

    def transform(self, column: pd.Series) -> pd.Series:
        return column + self.n
