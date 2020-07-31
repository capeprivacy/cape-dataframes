import pandas as pd
import pandas.testing as pdt
import pytest
from pyspark.sql import functions

from cape_privacy.spark import utils
from cape_privacy.spark.transformations import tokenizer as tkn


def _apply_tokenizer(sess, df, tokenizer, col_to_rename):
    df = sess.createDataFrame(df, schema=["name"])
    result_df = df.select(tokenizer(functions.col("name")))
    return result_df.withColumnRenamed(col_to_rename, "name").toPandas()


def test_tokenizer_simple():
    sess = utils.make_session("test.tokenizer.simple")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    expected = pd.DataFrame(
        {
            "name": [
                "70a4b1a987767abf36463cd3e3f2b37144132e572fbb9b39f28bcaafe10d9b24",
                "dd4532a296deb4f114b1e7e88faefe4fb2b32c559ac15a8c6bcbdbcbc2aa4d4b",
            ]
        }
    )
    key = "secret_key"
    df = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=key),
        col_to_rename="to_token(name)",
    )
    pdt.assert_frame_equal(df, expected)


def test_tokenizer_is_linkable():
    sess = utils.make_session("test.tokenizer.isLinkable")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    key1 = "secret_key"
    key2 = "secret_key"
    df1 = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=key1),
        col_to_rename="to_token(name)",
    )
    df2 = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=key2),
        col_to_rename="to_token(name)",
    )
    pdt.assert_frame_equal(df1, df2)


def test_tokenizer_is_not_linkable():
    sess = utils.make_session("test.tokenizer.isNotLinkable")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    key1 = "secret_key"
    key2 = "not_your_secret_key"
    df1 = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=key1),
        col_to_rename="to_token(name)",
    )
    df2 = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=key2),
        col_to_rename="to_token(name)",
    )
    try:
        pdt.assert_frame_equal(df1, df2)
        raise NotImplemented  # noqa: F901
    except AssertionError:
        pass
    except NotImplemented:
        raise AssertionError


def test_tokenizer_with_max_token_len():
    sess = utils.make_session("test.tokenizer.maxTokenLen")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    expected = pd.DataFrame({"name": ["70a4b1a987", "dd4532a296"]})
    max_token_len = 10
    key = "secret_key"
    df = _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=max_token_len, key=key),
        col_to_rename="to_token(name)",
    )
    pdt.assert_frame_equal(df, expected)


def test_tokenizer_no_key():
    sess = utils.make_session("test.tokenizer.maxTokenLen")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    _apply_tokenizer(
        sess,
        test_df,
        tkn.Tokenizer(max_token_len=None, key=None),
        col_to_rename="to_token(name)",
    )


def test_reversible_tokenizer():
    sess = utils.make_session("test.tokenizer.reversibleTokenizer")
    key = b"5" * 32
    plaintext = pd.DataFrame({"name": ["Alice", "Bob"]})

    tokenized = _apply_tokenizer(
        sess,
        plaintext,
        tkn.ReversibleTokenizer(key=key),
        col_to_rename="to_token(name)",
    )
    tokenized_expected = pd.DataFrame(
        {
            "name": [
                "c8c7e80144304276183e5bcd589db782bc5ff95309",
                "e0f40aea0d5c21b35967c4231b98b5b3e5338e",
            ]
        }
    )
    pdt.assert_frame_equal(tokenized, tokenized_expected)

    recovered = _apply_tokenizer(
        sess, tokenized, tkn.TokenReverser(key=key), col_to_rename="from_token(name)",
    )
    pdt.assert_frame_equal(recovered, plaintext)


def test_reversible_tokenizer_string_key():
    _ = tkn.ReversibleTokenizer(key="5" * 32)


def test_reversible_tokenizer_insufficient_key():
    with pytest.raises(ValueError):
        _ = tkn.ReversibleTokenizer(key=b"5" * 10)
