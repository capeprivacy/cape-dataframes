import numpy as np
import pandas as pd
from pyspark import sql

import cape_dataframes as cape

sess_builder = sql.SparkSession.builder
sess_builder = sess_builder.appName("cape.examples.rounding")
sess = sess_builder.getOrCreate()
sess = cape.spark.configure_session(sess)

pdf = pd.DataFrame(np.ones(5, dtype=np.float32) + 0.2, columns=["ones"])
df = sess.createDataFrame(pdf)
df.show()

policy = cape.parse_policy("policy/spark_round.yaml")
result = cape.apply_policy(policy, df)
result.show()
