from cape_privacy.pandas import registry
from cape_privacy.pandas.transformations import test_utils


def test_get():
    registry.register("plusN", test_utils.PlusN)
    ctor = registry.get("plusN")
    args = {"n": 1}
    ctor(**args)


def test_get_missing():
    try:
        ctor = registry.get("plusWhat?")
    except:
        raise AssertionError
    else:
        assert ctor is None
