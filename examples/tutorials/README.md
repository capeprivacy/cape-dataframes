# Tutorial: Mask Your Data in Pandas and Spark

`cape-privacy` gives you the ability to apply several masking techniques (transformations) such as tokenization, perturbation, rounding, etc., in order to obfuscate personal information contained in your dataset.

In this short tutorial, we will show you how you can prototype a masking policy on a Pandas DataFrame to then apply it on a Spark DataFrame.

## Experiment with masking techniques without a policy file

In order to get familiar with the different masking techniques and identify which one would fit your needs, you can apply these transformations directly on a Pandas DataFrame through the `cape_privacy.pandas.transformations` API without having to write the policy in a yaml file. 

For this example, we will use a mock dataset with the following PII fields: name, age, birthdate, salary and SSN. In order to obfuscate these different fields we will apply the following transformations:

- `name`: map each name to a unique token (`Tokenizer`). It will give the ability to obfuscate the name while maintaining user count in your dataset.
- `age`: add uniform random noise within the interval of `[-10, 10]` (`NumericPerturbation`).
- `birthdate`: add uniform random noise to year, month and day (`DatePerturbation`).
- `salary`: round each value to nearest 1,000 (`NumericRounding`).
- `SSN`: redact the field from the dataset (`ColumnRedact`).

You can experiment with these transformations on a Pandas DataFrame by running the following script:

```
python pandas_transformations_without_policy.py
```

You can also experiment with these transformations on Spark DataFrame with the `cape_privacy.pandas.transformations` API.

```
python spark_transformations_without_policy.py
```

As you will notice, the `transformations` API for `Pandas` and `Spark` are identitical, so you can easily transfer the transformations applied in `Pandas` to `Spark`.

## Write your policy

Once you identified the masking techniques you'd like to apply, you can define your policy in a `yaml` file. Below, you'll find a sample of the policy corresponding to the transformations applied in `pandas_transformations_without_policy.py`. You can find the full policy in `mask_personal_information.yaml`. You can select the field with `match` then define the transformation you'd like to apply under `transform` with the right arguments. The argument names in the policy file match the arguments of the `transformations` API. 

```yaml
label: masking_policy
version: 1
rules:
  - match:
      name: name
    actions:
      - transform:
          type: "tokenizer"
          max_token_len: 10
          key: "my secret"
  - match:
      name: age
    actions:
      - transform:
          type: "numeric-perturbation"
          dtype: Integer
          min: -10
          max: 10
```

## Apply the policy to your Spark DataFrame

You are now ready to apply the policy to your Spark DataFrame. You just need two methods:
- `cape_privacy.parse_policy`: load and parse the policy defined in the `yaml` file.
- `cape_privacy.apply_policy`: apply the policy to the Spark DataFrame (you can use the same method on a Pandas DataFrame).

To mask your data with Spark, simply run the following example:

```
python apply_policy_on_spark_df.py
```

You can also apply the exact same policy to a Pandas DataFrame:

```
python apply_policy_on_pandas_df.py
```

In your terminal, you should the see the data masked!
