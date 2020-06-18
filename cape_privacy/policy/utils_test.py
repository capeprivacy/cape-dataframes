import yaml

from .utils import yaml_args_to_kwargs

y = """
    n:
      value: 5
    key:
      value: random-key
    """


def test_yaml_args_to_kwargs():
    d = yaml.load(y, Loader=yaml.FullLoader)

    kwargs = yaml_args_to_kwargs(d)

    assert kwargs["n"] == 5
    assert kwargs["key"] == "random-key"
