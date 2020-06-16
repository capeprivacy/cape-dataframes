# Cape Python API

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
import pandas as pd
import numpy as np

import cape

df = pd.DataFrame(np.ones(5,), columns=["value"])

policy = cape.parse_policy("plus_one_value_field.yaml")
df = cape.apply_policies([policies], "transactions", df)

print(df.head())
```

You can also pass a URL to `parse_policy`.

```python
policy = cape.parse_policy("https://mydomain.com/policy.yaml")
```
