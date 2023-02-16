from cape_dataframes.pandas import registry
from cape_dataframes.pandas.transformations import test_utils


def test_get():
    registry.register("plusN", test_utils.PlusN)
    ctor = registry.get("plusN")
    args = {"n": 1}
    ctor(**args)


def test_get_missing():
    ctor = registry.get("plusWhat?")
    assert ctor is None
