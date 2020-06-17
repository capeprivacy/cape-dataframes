import numpy as np
import pandas as pd
from pyspark import sql

import cape_privacy as cape
from cape_privacy.pandas import policy
from cape_privacy.spark import transformations as tfms
from cape_privacy.spark import types

sess_builder = sql.SparkSession.builder
sess_builder = sess_builder.appName('cape.examples.rounding')
sess_builder = sess_builder.config('spark.sql.execution.arrow.enabled', 'true')
sess = sess_builder.getOrCreate()

pdf = pd.DataFrame(np.ones(5, dtype=np.float32) + .2, columns=["value"])
df = sess.createDataFrame(pdf)
df.show()

policy = cape.pandas.parse_policy("examples/spark_round.yaml")

rounding_spec = policy.transformations[0]
type_arg = getattr(types, rounding_spec.args['input_type']) 
rounding_spec.args['input_type'] = type_arg
rounder = tfms.Rounding(**rounding_spec.args)
udf = sql.functions.pandas_udf(rounder, returnType=rounder.type)
df = df.select(udf(sql.functions.col('value')))
df.show()
