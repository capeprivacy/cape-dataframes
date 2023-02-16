# Cape Dataframes

[![](https://github.com/capeprivacy/cape-dataframes/workflows/Main/badge.svg)](https://github.com/capeprivacy/cape-dataframes/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![codecov](https://codecov.io/gh/capeprivacy/cape-python/branch/master/graph/badge.svg?token=L9A8HFAJK5)](https://codecov.io/gh/capeprivacy/cape-python)
[![PyPI version](https://badge.fury.io/py/cape-privacy.svg)](https://badge.fury.io/py/cape-privacy)
[![Cape Community Discord](https://img.shields.io/discord/1027271440061435975)](https://discord.gg/nQW7YxUYjh)

A Python library supporting data transformations and collaborative privacy policies, for data science projects in Pandas and Apache Spark

See below for instructions on how to get started or visit the [documentation](https://github.com/capeprivacy/cape-dataframes/tree/master/docs/).

## Getting started

### Prerequisites

* Python 3.6 or above, and pip
* Pandas 1.0+
* PySpark 3.0+ (if using Spark)
* [Make](https://www.gnu.org/software/make/) (if installing from source)

### Install with pip

Cape Dataframes is available through PyPi.

```sh
pip install cape-dataframes
```

Support for Apache Spark is optional.  If you plan on using the library together with Apache Spark, we suggest the following instead:

```sh
pip install cape-dataframes[spark]
```

We recommend running it in a virtual environment, such as [venv](https://docs.python.org/3/library/venv.html).

### Install from source

It is possible to install the library from source. This installs all dependencies, including Apache Spark:

```sh
git clone https://github.com/capeprivacy/cape-dataframes.git
cd cape-dataframes
make bootstrap
```
### Usage example

*This example is an abridged version of the tutorial found [here](https://github.com/capeprivacy/cape-dataframes/tree/master/examples/tutorials)*


```python
df = pd.DataFrame({
    "name": ["alice", "bob"],
    "age": [34, 55],
    "birthdate": [pd.Timestamp(1985, 2, 23), pd.Timestamp(1963, 5, 10)],
})

tokenize = Tokenizer(max_token_len=10, key=b"my secret")
perturb_numeric = NumericPerturbation(dtype=dtypes.Integer, min=-10, max=10)

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

You can see more [examples and usage](https://github.com/capeprivacy/cape-dataframes/tree/master/examples/) or read our [documentation](https://github.com/capeprivacy/cape-dataframes/tree/master/docs/).

## About Cape Privacy and Cape Dataframes

[Cape Privacy](https://capeprivacy.com) empowers developers to easily encrypt data and process it confidentially. No cryptography or key management required.. Learn more at [capeprivacy.com](https://capeprivacy.com).

Cape Dataframes brings Cape's policy language to Pandas and Apache Spark. The supported techniques include tokenization with linkability as well as perturbation and rounding. You can experiment with these techniques programmatically, in Python or in human-readable policy files.

### Project status and roadmap

Cape Python 0.1.1 was released 24th June 2020. It is actively maintained and developed, alongside other elements of the Cape ecosystem.

**Upcoming features:**

* Reversible tokenisation: allow reversing of tokenization to reveal the raw value.
* Expand pipeline integrations: add Apache Beam, Apache Flink, Apache Arrow Flight or Dask integration as another pipeline we can support, either as part of Cape Dataframes or in its own separate project.

## Help and resources

If you need help using Cape Dataframes, you can:

* View the [documentation](https://github.com/capeprivacy/cape-dataframes/tree/master/docs/).
* Submit an issue.
* Talk to us on the [Cape Community Discord](https://discord.gg/nQW7YxUYjh) [![Cape Community Discord](https://img.shields.io/discord/1027271440061435975)](https://discord.gg/nQW7YxUYjh)

Please file [feature requests](https://github.com/capeprivacy/cape-dataframes/issues/new?template=feature_request.md) and 
[bug reports](https://github.com/capeprivacy/cape-dataframes/issues/new?template=bug_report.md) as GitHub issues.

### Contributing

View our [contributing](CONTRIBUTING.md) guide for more information.

### Code of conduct

Our [code of conduct](https://capeprivacy.com/conduct/) is included on the Cape Privacy website. All community members are expected to follow it. Please refer to that page for information on how to report problems.

## License

Licensed under Apache License, Version 2.0 (see [LICENSE](https://github.com/capeprivacy/cape-python/blob/master/LICENSE) or http://www.apache.org/licenses/LICENSE-2.0). Copyright as specified in [NOTICE](https://github.com/capeprivacy/cape-python/blob/master/NOTICE).
