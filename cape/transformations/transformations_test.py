from .transformations import get


def test_get():
    ctor = get("plusOne")
    ctor("cool_field", {})
