import cape_privacy as cape
from dataset import load_dataset


# Load the Spark DataFrame
df = load_dataset(framework="spark")
print("Original Dataset:")
print(df.show())
# Load the privacy policy
policy = cape.parse_policy("mask_personal_information.yaml")
# Apply the policy to the DataFrame
df = cape.apply_policy(policy, df)
# Output the masked dataset
print("Masked Dataset:")
print(df.show())
