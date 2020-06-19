from .transformations import get


def test_get():
    ctor = get("plusN")
    args = {"n": 1}
    ctor(**args)
