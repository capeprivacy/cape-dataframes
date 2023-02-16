from pyspark import sql
from pyspark.sql import functions

from cape_privacy.spark.transformations import base as tfm


def transformer(transformation: tfm.Transformation, df: sql.DataFrame, field_name: str):
    field_column = functions.col(field_name)
    return df.withColumn(field_name, transformation(field_column))
