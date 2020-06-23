import pandas as pd

from cape_privacy.pandas.transformations import base as tfm


def transformer(transformation: tfm.Transformation, df: pd.DataFrame, field_name: str):
    df[field_name] = transformation(df[field_name])
    return df
