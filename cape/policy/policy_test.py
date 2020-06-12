import numpy as np
import pandas as pd
import pandas.testing as pdt
import yaml

from .policy import apply_policies
from .policy import get_transformations

y = """
    id: 2
    created_at: 5
    updated_at: 5
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  function: plusOne
                  args: []
                - field: test
                  function: plusOne
                  args: []
    """


def test_get_transformations():
    d = yaml.load(y, Loader=yaml.FullLoader)

    transforms = get_transformations(d["spec"]["rules"][0])

    assert len(transforms) == 2
    assert transforms[0].field == "test"
    assert transforms[0].function == "plusOne"


def test_apply_policies():
    d = yaml.load(y, Loader=yaml.FullLoader)

    df = pd.DataFrame(np.ones(5,), columns=["test"])

    # policy gets applied first
    expected_df = df + 2

    new_df = apply_policies([d], "transactions", df)

    pdt.assert_frame_equal(new_df, expected_df)
