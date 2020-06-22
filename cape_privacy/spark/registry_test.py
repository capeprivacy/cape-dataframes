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
    tfm_cls = registry.get("mock")
    args = {"fake_arg": 1}
    tfm_cls(**args)
    registry._registry.pop("mock")


def test_get_missing():
    tfm_cls = registry.get("plusWhat?")
    assert tfm_cls is None
