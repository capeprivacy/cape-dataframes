import numpy as np
import pandas as pd

String = pd.api.types.pandas_dtype(str)
Date = pd.api.types.pandas_dtype("datetime64")
# numeric types
Float = pd.api.types.pandas_dtype(np.float32)
Double = pd.api.types.pandas_dtype(np.float64)
Byte = pd.api.types.pandas_dtype(np.byte)
Short = pd.api.types.pandas_dtype(np.short)
Integer = pd.api.types.pandas_dtype(np.int32)
Long = pd.api.types.pandas_dtype(np.int64)
# groupings
Floats = (Float, Double)
Integers = (Byte, Short, Integer, Long)
Numerics = Floats + Integers
