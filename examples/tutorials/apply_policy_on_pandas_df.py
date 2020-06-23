
import cape_privacy as cape
import pandas as pd

from dataset import load_dataset


# Load the Pandas DataFrame
df = load_dataset()
print("Original Dataset:")
print(df.head())
# Load the privacy policy and apply it to the DataFrame
policy = cape.parse_policy("mask_personal_information.yaml")
df = cape.apply_policy(policy, df)

print("Masked Dataset:")
print(df.head())
