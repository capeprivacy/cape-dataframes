from cape_privacy.spark import registry
from cape_privacy.spark.transformations import base


class MockTransformation(base.Transformation):
    identifier = "mock"

    def __init__(self, fake_arg):
        super().__init__(None)

    def __call__(self, x):
        pass


def test_get():
    registry.register(MockTransformation.identifier, MockTransformation)
    ctor = registry.get("mock")
    args = {"fake_arg": 1}
    ctor(**args)
    registry._registry.pop("mock")


def test_get_missing():
    try:
        ctor = registry.get("plusWhat?")
    except:
        raise AssertionError
    else:
        assert ctor is None
