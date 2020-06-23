# Cape Python

![](https://github.com/capeprivacy/cape-python/workflows/Main/badge.svg) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![codecov](https://codecov.io/gh/capeprivacy/cape-python/branch/master/graph/badge.svg?token=L9A8HFAJK5)](https://codecov.io/gh/capeprivacy/cape-python)


## Getting Started

Make sure you have at least Python 3.6 installed. We recommend running it in a virtual environment
such as with [venv](https://docs.python.org/3/library/venv.html) or
[conda](https://www.anaconda.com/products/individual).

`make` will also be required to run our `Makefile` so ensure that you have that installed as well.

### Bootstrapping

Bootstrapping your environment installs all direct dependencies of the Python API
and ensures that the API is installed as well.

Run the following command from a command line:

```bash
$ make bootstrap
```

#### Example

This example does a basic plusOne transformation on a pandas dataframe with a single column called `value`. It can be
found in the `examples` directory.

```python
import cape_privacy as cape
import numpy as np
import pandas as pd

policy = cape.parse_policy("perturb_value_field.yaml")
df = pd.DataFrame(np.ones(5,), columns=["value"])
df = cape.apply_policy(policy, df)

print(df.head())
```

You can also pass a URL to `parse_policy`.

```python
policy = cape.parse_policy("https://mydomain.com/policy.yaml")
```

# License

Licensed under Apache License, Version 2.0 (see [LICENSE](./LICENSE) or http://www.apache.org/licenses/LICENSE-2.0). Copyright as specified in [NOTICE](./NOTICE).
