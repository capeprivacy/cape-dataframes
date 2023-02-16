from dataset import load_dataset
from pyspark import sql

import cape_dataframes as cape

# Set up your SparkSession as usual, but configure it for use with Cape.
# We do this because some transformations expect Arrow to be enabled.
sess = sql.SparkSession.builder.appName(
    "cape.tutorial.maskPersonalInformation"
).getOrCreate()
sess = cape.spark.configure_session(sess)

# Load a Spark DataFrame
df = load_dataset(sess)
print("Original Dataset:")
print(df.show())
# Load the privacy policy and apply it to the DataFrame
policy = cape.parse_policy("mask_personal_information.yaml")
df = cape.apply_policy(policy, df)

print("Masked Dataset:")
print(df.show())
