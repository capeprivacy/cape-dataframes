import cape_privacy
from dataset import load_dataset


# Load the Spark DataFrame
df = load_dataset(framework="spark")
print("Original Dataset:")
print(df.show())
# Load the privacy policy
policy = cape_privacy.parse_policy("mask_personal_information.yaml")
# Apply the policy to the DataFrame
# [NOTE] will be updated to `cape_privacy.apply_policy` #49 is merged
df = cape_privacy.apply_policy(policy, df)
# Output the masked dataset
print("Masked Dataset:")
print(df.show())
