import pandas as pd
from pyspark import sql
from pyspark.sql import functions
from pyspark.sql import types

spark = sql.SparkSession.builder.appName("Basic").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
print("Using Arrow?", spark.conf.get("spark.sql.execution.arrow.enabled"))

# Declare the function and create the UDF
def multiply_func(a, b):
    return a * b

multiply = functions.pandas_udf(multiply_func, returnType=types.LongType())

# The function for a pandas_udf should be able to execute with local Pandas data
x = pd.Series([1, 2, 3])
print(multiply_func(x, x))
# 0    1
# 1    4
# 2    9
# dtype: int64

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# Execute function as a Spark vectorized UDF
df.select(multiply(functions.col("x"), functions.col("x"))).show()
