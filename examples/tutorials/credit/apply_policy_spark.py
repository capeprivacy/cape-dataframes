from pyspark import sql
from pyspark.sql import functions

import cape_privacy as cape

# Set up your SparkSession as usual, but configure it for use with Cape.
# We do this because some transformations expect Arrow to be enabled.
sess = sql.SparkSession.builder.appName(
    "cape.tutorial.maskPersonalInformation"
).getOrCreate()
sess = cape.spark.configure_session(sess)

# Load a Spark DataFrame
df = sess.read.load(
    "data/credit_with_pii.csv", format="csv", sep=",", inferSchema="true", header="true"
)
df = df.withColumn(
    "Application_date",
    functions.to_date(functions.col("Application_date"), "yyyy-MM-dd"),
)
print("Original Dataset:")
print(df.show())
# Load the privacy policy and apply it to the DataFrame
policy = cape.parse_policy("policy/credit_policy.yaml")
df = cape.apply_policy(policy, df)

print("Masked Dataset:")
print(df.show())
