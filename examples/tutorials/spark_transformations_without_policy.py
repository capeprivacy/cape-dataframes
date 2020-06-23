from pyspark.sql import functions

from cape_privacy.spark import dtypes
from cape_privacy.spark.transformations import ColumnRedact
from cape_privacy.spark.transformations import DatePerturbation
from cape_privacy.spark.transformations import NumericRounding
from cape_privacy.spark.transformations import NumericPerturbation
from cape_privacy.spark.transformations import Tokenizer

from dataset import load_dataset


# Load Spark DataFrame
df = load_dataset(framework="spark")
print("Original Dataset:")
df.show()


# Define the transformations
tokenize = Tokenizer(
    max_token_len=10,
    key=b"my secret",
)

perturb_numric = NumericPerturbation(
    dtype=dtypes.Integer,
    min=-10,
    max=10,
)

perturb_date = DatePerturbation(
    frequency=("YEAR", "MONTH", "DAY"),
    min=(-10, -5, -5),
    max=(10, 5, 5),
)

round_numeric = NumericRounding(
    dtype=dtypes.Float,
    precision=-3
)

redact_column = ColumnRedact(columns="ssn")


# Apply the transformation
df = redact_column(df)
df = df.select(tokenize(functions.col('name')).alias('name'),
               perturb_numric(functions.col('age')).alias('age'),
               round_numeric(functions.col('salary')).alias('salary'),
               perturb_date(functions.col('birthdate')).alias('birthdate'))

print("Masked Dataset:")
print(df.show())
