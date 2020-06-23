import numpy as np
import pandas as pd
from pyspark import sql
from pyspark.sql import functions

import cape_privacy as cape
from cape_privacy.spark import transformations as tfms
from cape_privacy.spark import dtypes

sess_builder = sql.SparkSession.builder
sess_builder = sess_builder.appName('cape.examples.rounding')
sess_builder = sess_builder.config('spark.sql.execution.arrow.enabled', 'true')
sess = sess_builder.getOrCreate()

pdf = pd.DataFrame(np.ones(5, dtype=np.float32) + .2, columns=["value"])
df = sess.createDataFrame(pdf)
df.show()

policy = cape.parse_policy("spark_round.yaml")
rounding_spec = policy.transformations[0]
type_arg = getattr(dtypes, rounding_spec.args['dtype'])
rounding_spec.args['dtype'] = type_arg
rounder = tfms.NumericRounding(**rounding_spec.args)
df = df.select(rounder(functions.col('value')))
df.show()
