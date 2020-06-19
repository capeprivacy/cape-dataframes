[<img src="https://raw.githubusercontent.com/dropoutlabs/files/master/cape-logo.png" alt="Cape Privacy" width="500"/>](https://capeprivacy.com/)

![](https://github.com/capeprivacy/cape-python/workflows/Main/badge.svg) 
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![codecov](https://codecov.io/gh/capeprivacy/cape-python/branch/master/graph/badge.svg?token=L9A8HFAJK5)](https://codecov.io/gh/capeprivacy/cape-python)
[![PyPI version](https://badge.fury.io/py/cape-privacy.svg)](https://badge.fury.io/py/cape-privacy)

Cape Privacy offers data scientists and data engineers a policy-based interface for applying privacy-enhancing techniques 
across several popular libraries and frameworks to protect sensitive data throughout the data science life cycle.

Cape Python brings Cape's policy language to Pandas and Apache Spark, 
enabling you to collaborate on privacy-preserving policy at a non-technical level. 
The supported techniques include tokenization with linkability as well as perturbation and rounding.
You can experiment with these techniques programmatically, in Python or in human-readable policy files. 
Stay tuned for more privacy-enhancing techniques in the future!

See below for instructions on how to get started or visit the [documentation](https://docs.capeprivacy.com/).

## Getting Started

Cape Python is available via Pypi.

```sh
pip install cape-privacy
```

Support for Apache Spark is optional.  If you plan on using the library together with Apache Spark, we suggest the following instead:

```sh
pip install cape-privacy[spark]
```

We recommend running it in a virtual environment, such as [venv](https://docs.python.org/3/library/venv.html).

### Installing from source

It is also possible to install the library from source.

```sh
git clone https://github.com/capeprivacy/cape-python.git
cd cape-python
make bootstrap
```

This will also install all dependencies, including Apache Spark. Make sure you have `make` installed before running the above.

## Example

*(this example is an abridged version of the tutorial found [here](./examples/tutorials/))*

To discover what different transformations do and how you might use them, it is best to explore via the `transformations` APIs:

```python
df = pd.DataFrame({
        "name": ["alice", "bob"],
        "age": [34, 55],
        "birthdate": [pd.Timestamp(1985, 2, 23), pd.Timestamp(1963, 5, 10)],
    })

tokenize = Tokenizer(
    max_token_len=10,
    key=b"my secret",
)

perturb_numeric = NumericPerturbation(
    dtype=dtypes.Integer,
    min=-10,
    max=10,
)

df["name"] = tokenize(df["name"])
df["age"] = perturb_numeric(df["age"])

print(df.head())

# >>
#          name  age  birthdate
# 0  f42c2f1964   34 1985-02-23
# 1  2e586494b2   63 1963-05-10
```

These steps can be saved in policy files so you can share them and collaborate with your team:

```yaml
# my-policy.yaml
label: my-policy
version: 1
rules:
  - match:
      name: age
    actions:
      - transform:
          type: numeric-perturbation
          dtype: Integer
          min: -10
          max: 10
          seed: 4984
  - match:
      name: name
    actions:
      - transform:
          type: tokenizer
          max_token_len: 10
          key: my secret
``` 

You can then load this policy and apply it to your data frame:

```python
# df can be a Pandas or Spark data frame 
policy = cape.parse_policy("my-policy.yaml")
df = cape.apply_policy(policy, df)

print(df.head())
# >>
#          name  age  birthdate
# 0  f42c2f1964   34 1985-02-23
# 1  2e586494b2   63 1963-05-10
```

You can see more examples and usage [here](./examples) or by visiting our [documentation](https://docs.capeprivacy.com).

## Contributing and Bug Reports

Please file any [feature request](https://github.com/capeprivacy/cape-python/issues/new?template=feature_request.md) or 
[bug report](https://github.com/capeprivacy/cape-python/issues/new?template=bug_report.md) as GitHub issues.

## License

Licensed under Apache License, Version 2.0 (see [LICENSE](./LICENSE) or http://www.apache.org/licenses/LICENSE-2.0). Copyright as specified in [NOTICE](./NOTICE).

## About Cape

[Cape Privacy](https://capeprivacy.com) helps teams share data and make decisions for safer and more powerful data science. Learn more at [capeprivacy.com](https://capeprivacy.com).
