from typing import Any
from typing import Dict

import pandas as pd


class PlusOne:
    function = "plusOne"

    def __init__(self, field: str, args: Dict[Any, Any]) -> None:
        self.field = field

    def transform(self, column: pd.Series) -> pd.Series:
        return column + 1
