from dataset import load_dataset

from cape_privacy.pandas import dtypes
from cape_privacy.pandas.transformations import ColumnRedact
from cape_privacy.pandas.transformations import DatePerturbation
from cape_privacy.pandas.transformations import NumericPerturbation
from cape_privacy.pandas.transformations import NumericRounding
from cape_privacy.pandas.transformations import Tokenizer

# Load Pandas DataFrame
df = load_dataset()
print("Original Dataset:")
print(df.head())

# Define the transformations
tokenize = Tokenizer(max_token_len=10, key=b"my secret")
perturb_numric = NumericPerturbation(dtype=dtypes.Integer, min=-10, max=10)
perturb_date = DatePerturbation(
    frequency=("YEAR", "MONTH", "DAY"), min=(-10, -5, -5), max=(10, 5, 5)
)
round_numeric = NumericRounding(dtype=dtypes.Float, precision=-3)
redact_column = ColumnRedact(columns="ssn")

# Apply the transformations
df["name"] = tokenize(df["name"])
df["age"] = perturb_numric(df["age"])
df["salary"] = round_numeric(df["salary"])
df["birthdate"] = perturb_date(df["birthdate"])
df = redact_column(df)

print("Masked Dataset:")
print(df.head())
