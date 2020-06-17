import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

def python_func(a, b):
    print("foo", len(a), len(b))
    return a * b + a * b

spark_func = pandas_udf(python_func, returnType=LongType())

x = pd.Series([x for x in range(100)])
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

#df = df.select(col("x"), python_func(col("x"), col("x")))
df = df.select(col("x"), spark_func(col("x"), col("x")))

df.show()
exit()

import time
start = time.time()
for _ in range(100):
    df.collect()
end = time.time()
print(end - start)
