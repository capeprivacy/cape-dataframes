import pandas as pd
import pandas.testing as pdt
import pytest

from cape_privacy.pandas.transformations import ReversibleTokenizer
from cape_privacy.pandas.transformations import Tokenizer
from cape_privacy.pandas.transformations import TokenReverser


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


def test_reversible_tokenizer():
    key = b"5" * 32
    plaintext = pd.DataFrame({"name": ["Alice", "Bob"]})

    tokenizer = ReversibleTokenizer(key=key)
    tokenized_expected = pd.DataFrame(
        {
            "name": [
                "c8c7e80144304276183e5bcd589db782bc5ff95309",
                "e0f40aea0d5c21b35967c4231b98b5b3e5338e",
            ]
        }
    )
    tokenized = pd.DataFrame()
    tokenized["name"] = tokenizer(plaintext["name"])
    pdt.assert_frame_equal(tokenized, tokenized_expected)

    reverser = TokenReverser(key=key)
    recovered = pd.DataFrame()
    recovered["name"] = reverser(tokenized["name"])
    pdt.assert_frame_equal(recovered, plaintext)


def test_reversible_tokenizer_string_key():
    _ = ReversibleTokenizer(key="5" * 32)


def test_reversible_tokenizer_insufficient_key():
    with pytest.raises(ValueError):
        _ = ReversibleTokenizer(key=b"5" * 10)
