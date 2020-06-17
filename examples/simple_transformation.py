import pandas as pd
import numpy as np

from cape_privacy import pandas as cape

df = pd.DataFrame(np.ones(5,), columns=["value"])

policy = cape.parse_policy("plus_one_value_field.yaml")
df = cape.apply_policies([policy], "transactions", df)

print(df.head())