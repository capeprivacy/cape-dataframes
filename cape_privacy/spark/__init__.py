import importlib

if importlib.util.find_spec("pyspark") is None:

    def is_available():
        return False

    __all__ = ["is_available"]

else:

    from pyspark.sql import DataFrame

    from cape_privacy.spark import dtypes
    from cape_privacy.spark import registry
    from cape_privacy.spark import transformations
    from cape_privacy.spark.transformer import transformer
    from cape_privacy.spark.utils import configure_session
    from cape_privacy.spark.utils import make_session

    def is_available():
        return True

    __all__ = [
        "configure_session",
        "DataFrame",
        "dtypes",
        "is_available",
        "make_session",
        "transformations",
        "transformer",
        "registry",
    ]

del importlib
