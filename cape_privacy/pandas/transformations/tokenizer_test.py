import pandas as pd
import pandas.testing as pdt

from cape_privacy.pandas.transformations import Tokenizer


def test_tokenizer():
    transform = Tokenizer(key="secret_key")

    df = pd.DataFrame({"name": ["Alice", "Bob"]})
    expected = pd.DataFrame(
        {
            "name": [
                "70a4b1a987767abf36463cd3e3f2b37144132e572fbb9b39f28bcaafe10d9b24",
                "dd4532a296deb4f114b1e7e88faefe4fb2b32c559ac15a8c6bcbdbcbc2aa4d4b",
            ]
        }
    )

    df["name"] = transform(df["name"])

    pdt.assert_frame_equal(df, expected)


def test_tokenizer_with_max_size():
    transform = Tokenizer(max_token_len=10, key="secret_key")

    df = pd.DataFrame({"name": ["Alice", "Bob"]})
    expected = pd.DataFrame({"name": ["70a4b1a987", "dd4532a296"]})

    df["name"] = transform(df["name"])

    pdt.assert_frame_equal(df, expected)
