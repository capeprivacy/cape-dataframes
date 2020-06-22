import importlib

if importlib.util.find_spec("pyspark") is None:

    def is_available():
        return False

    __all__ = ["is_available"]

else:

    from pyspark.sql import DataFrame

    from cape_privacy.spark import dtypes
    from cape_privacy.spark import transformations
    from cape_privacy.spark.policy import apply_policies

    def is_available():
        return True

    __all__ = [
        "apply_policies",
        "DataFrame",
        "dtypes",
        "is_available",
        "transformations",
    ]

del importlib
