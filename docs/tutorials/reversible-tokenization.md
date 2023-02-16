# Reversible Tokenizer

Here we show an example of how you can use the `ReversibleTokenizer` to tokenize data within a pandas dataframe.

The `ReversibleTokenizer` will tokenize the input data so it can be used in a privacy preserving manner.

The `ReversibleTokenizer` can be used in conjunction with the `TokenReverser` to recover the original data.

## Tokenizing Data

The `ReversibleTokenizer` and `TokenReverser` classes can be found in the `pandas.transformations` package.

```python
from cape_privacy.pandas.transformations import ReversibleTokenizer
from cape_privacy.pandas.transformations import TokenReverser
```

In this example, we will simply hide the names within our dataset.

```python
import pandas as pd
plaintext_data = pd.DataFrame({'name': ["Alice", "Bob", "Carol"], "# friends": [100, 200, 300]})
```

You instantiate a `ReversibleTokenizer` by passing it a key. For the `TokenReverser` to be able to reverse the tokens produced by the `ReversibleTokenizer`, you must use the same key.

```python
key=b"5" * 32
tokenizer = ReversibleTokenizer(key=key)
```

```python
tokenized = pd.DataFrame(plaintext_data)
tokenized["name"] = tokenizer(plaintext_data["name"])
```

## Recovering Tokens

If we ever need to reveal the tokenized data, we can use the `TokenReverser` class.

```python
reverser = TokenReverser(key=key)
recovered = pd.DataFrame(tokenized)
recovered["name"] = reverser(tokenized["name"])
```

You can see full code for this example on [Github](https://github.com/capeprivacy/cape-dataframes/blob/master/examples/tutorials/reversible_tokenizer/reversible_tokenizer_pandas.ipynb)
