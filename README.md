# Cape Python

![](https://github.com/capeprivacy/cape-python/workflows/Main/badge.svg) 
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![codecov](https://codecov.io/gh/capeprivacy/cape-python/branch/master/graph/badge.svg?token=L9A8HFAJK5)](https://codecov.io/gh/capeprivacy/cape-python)
[![PyPI version](https://badge.fury.io/py/cape-privacy.svg)](https://badge.fury.io/py/cape-privacy)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20slack-7A5979.svg)](https://join.slack.com/t/capecommunity/shared_invite/zt-f8jeskkm-r9_FD0o4LkuQqhJSa~~IQA)


A Python library supporting data transformations and collaborative privacy policies, for data science projects in Pandas and Apache Spark

See below for instructions on how to get started or visit the [documentation](https://docs.capeprivacy.com/).

## Getting started

### Prerequisites

* Python 3.6 or above, and pip
* Pandas 1.0+
* PySpark 3.0+ (if using Spark)
* [Make](https://www.gnu.org/software/make/) (if installing from source)

### Install with pip

Cape Python is available through PyPi.

```sh
pip install cape-privacy
```

Support for Apache Spark is optional.  If you plan on using the library together with Apache Spark, we suggest the following instead:

```sh
pip install cape-privacy[spark]
```

We recommend running it in a virtual environment, such as [venv](https://docs.python.org/3/library/venv.html).

### Install from source

It is possible to install the library from source. This installs all dependencies, including Apache Spark:

```sh
git clone https://github.com/capeprivacy/cape-python.git
cd cape-python
make bootstrap
```
### Usage example

*This example is an abridged version of the tutorial found [here](https://github.com/capeprivacy/cape-python/tree/master/examples/tutorials)*


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

You can see more examples and usage [here](https://github.com/capeprivacy/cape-python/tree/master/examples/) or in our [documentation](https://docs.capeprivacy.com).

## About Cape Privacy and Cape Python

[Cape Privacy](https://capeprivacy.com) helps teams share data and make decisions for safer and more powerful data science. Learn more at [capeprivacy.com](https://capeprivacy.com).

Cape Python brings Cape's policy language to Pandas and Apache Spark. The supported techniques include tokenization with linkability as well as perturbation and rounding. You can experiment with these techniques programmatically, in Python or in human-readable policy files.

### Cape architecture

Cape is comprised of multiples services and libraries. You can use Cape Python as a standalone library, or you can integrate it with the Coordinator in [Cape Core](https://github.com/capeprivacy/cape/), which supports user and policy management.

### Project status and roadmap

Cape Python 0.1.1 was released 24th June 2020. It is actively maintained and developed, alongside other elements of the Cape ecosystem.

**Upcoming features:**

* Reversible tokenisation: allow reversing of tokenization to reveal the raw value.
* Policy audit logging: create logging hooks to allow audit logs for policy downloads and usage in Cape Python.
* Expand pipeline integrations: add Apache Beam, Apache Flink, Apache Arrow Flight or Dask integration as another pipeline we can support, either as part of Cape Python or in its own separate project.

The goal is a complete data management ecosystem. Cape Privacy provides [Cape Coordinator](https://docs.capeprivacy.com/cape-core/coordinator/), to manage policy and users. This will interact with the Cape Privacy libraries (such as [Cape Python](https://docs.capeprivacy.com/libraries/cape-python/)) through a workers interface, and with your own data services through an API.


## Help and resources

If you need help using Cape Python, you can:

* View the [documentation](https://docs.capeprivacy.com/).
* Submit an issue.
* Talk to us on our [community Slack](https://join.slack.com/t/capecommunity/shared_invite/zt-f8jeskkm-r9_FD0o4LkuQqhJSa~~IQA).

Please file [feature requests](https://github.com/capeprivacy/cape-python/issues/new?template=feature_request.md) and 
[bug reports](https://github.com/capeprivacy/cape-python/issues/new?template=bug_report.md) as GitHub issues.

## Community

[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/0)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/0)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/1)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/1)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/2)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/2)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/3)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/3)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/4)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/4)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/5)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/5)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/6)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/6)[![](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/images/7)](https://sourcerer.io/fame/justin1121/capeprivacy/cape-python/links/7)

### Contributing

View our [contributing](CONTRIBUTING.md) guide for more information.

### Code of conduct

Our [code of conduct](https://capeprivacy.com/conduct/) is included on the Cape Privacy website. All community members are expected to follow it. Please refer to that page for information on how to report problems.

## License

Licensed under Apache License, Version 2.0 (see [LICENSE](https://github.com/capeprivacy/cape-python/blob/master/LICENSE) or http://www.apache.org/licenses/LICENSE-2.0). Copyright as specified in [NOTICE](https://github.com/capeprivacy/cape-python/blob/master/NOTICE).


