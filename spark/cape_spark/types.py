from pyspark.sql import types

String = types.StringType
Date = types.DateType
# numeric types
Float = types.FloatType
Double = types.DoubleType
Byte = types.ByteType
Short = types.ShortType
Integer = types.IntegerType
Long = types.LongType
# groupings
Floats = (Float, Double)
Integers = (Byte, Short, Integer, Long)
Numerics = (Float, Double, Byte, Short, Integer, Long)
