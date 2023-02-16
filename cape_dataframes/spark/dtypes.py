from pyspark.sql import types

# base type
DType = types.DataType
# individual types
String = types.StringType()
Date = types.DateType()
Datetime = types.TimestampType()
# numeric types
Float = types.FloatType()
Double = types.DoubleType()
Byte = types.ByteType()
Short = types.ShortType()
Integer = types.IntegerType()
Long = types.LongType()
# groups
Floats = (Float, Double)
Integers = (Byte, Short, Integer, Long)
Numerics = Floats + Integers
