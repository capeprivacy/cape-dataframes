import pandas as pd
import numpy as np

import cape_privacy.pandas as cape

df = pd.DataFrame(np.ones(5,), columns=["value"])

policy = cape.parse_policy("perturb_value_field.yaml")
df = cape.apply_policies([policy], df)

print(df.head())
