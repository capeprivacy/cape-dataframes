import pandas as pd
import pandas.testing as pdt
from pyspark.sql import functions

from cape_privacy.spark import utils
from cape_privacy.spark.transformations import tokenizer as tkn


def _make_and_apply_tokenizer(sess, df, max_token_len, key):
    df = sess.createDataFrame(df, schema=["name"])
    tokenize = tkn.Tokenizer(max_token_len, key)
    result_df = df.select(tokenize(functions.col("name")))
    return result_df.withColumnRenamed("to_token(name)", "name").toPandas()


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
    df = _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=key)
    pdt.assert_frame_equal(df, expected)


def test_tokenizer_is_linkable():
    sess = utils.make_session("test.tokenizer.isLinkable")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    key1 = "secret_key"
    key2 = "secret_key"
    df1 = _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=key1)
    df2 = _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=key2)
    pdt.assert_frame_equal(df1, df2)


def test_tokenizer_is_not_linkable():
    sess = utils.make_session("test.tokenizer.isNotLinkable")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    key1 = "secret_key"
    key2 = "not_your_secret_key"
    df1 = _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=key1)
    df2 = _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=key2)
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
    df = _make_and_apply_tokenizer(sess, test_df, max_token_len=max_token_len, key=key)
    pdt.assert_frame_equal(df, expected)


def test_tokenizer_no_key():
    sess = utils.make_session("test.tokenizer.maxTokenLen")
    test_df = pd.DataFrame({"name": ["Alice", "Bob"]})
    _make_and_apply_tokenizer(sess, test_df, max_token_len=None, key=None)
