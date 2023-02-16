# This is a pure python version of the notebook from this directory

# Here we show an example of how you can use the `ReversibleTokenizer` to tokenize data
# within a pandas dataframe. The `ReversibleTokenizer` will tokenize the input data so
# it can be used in a privacy-preserving manner. The `ReversibleTokenizer` can be used
# in conjunction with the `TokenReverser` to recover the original data.

import pandas as pd

from cape_dataframes.pandas.transformations import ReversibleTokenizer
from cape_dataframes.pandas.transformations import TokenReverser

# The `ReversibleTokenizer` and `TokenReverser` classes both take a `key` as input.
# For the `TokenReverser` to be able to reverse the tokens produced by the
# `ReversibleTokenizer`, you must use the same key.

key = b"5" * 32

# In this example, we will simply hide the names within our dataset.
plaintext_data = pd.DataFrame(
    {"name": ["Alice", "Bob", "Carol"], "# friends": [100, 200, 300]}
)
print("plantext data")
print(plaintext_data)
print("\n")

# Tokenization logic
tokenizer = ReversibleTokenizer(key=key)
tokenized = pd.DataFrame(plaintext_data)
tokenized["name"] = tokenizer(plaintext_data["name"])

print("tokenized data")
print(plaintext_data)
print("\n")

# Reverse the tokenization
reverser = TokenReverser(key=key)
recovered = pd.DataFrame(tokenized)
recovered["name"] = reverser(tokenized["name"])

print("reversed tokens")
print(recovered)
print("\n")
