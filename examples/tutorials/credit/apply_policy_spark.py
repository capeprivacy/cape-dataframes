import cape_privacy as cape
import pandas as pd
from pyspark import sql

# Set up your SparkSession as usual, but configure it for use with Cape.
# We do this because some transformations expect Arrow to be enabled.
sess = sql.SparkSession.builder \
    .appName("cape.tutorial.maskPersonalInformation") \
    .getOrCreate()
sess = cape.spark.configure_session(sess)

# Load a Spark DataFrame
pdf = pd.read_csv('data/credit_with_pii.csv')
pdf.Application_date = pd.to_datetime(pdf.Application_date)
df = sess.createDataFrame(pdf)
print("Original Dataset:")
print(df.show())
# Load the privacy policy and apply it to the DataFrame
policy = cape.parse_policy("policy/credit_policy.yaml")
df = cape.apply_policy(policy, df)

print("Masked Dataset:")
print(df.show())
