import numpy as np
import pandas as pd

import cape_privacy as cape

policy = cape.parse_policy("policy/perturb_value_field.yaml")

df = pd.DataFrame(
    np.ones(
        5,
    ),
    columns=["ones"],
)
df = cape.apply_policy(policy, df)
print(df.head())
