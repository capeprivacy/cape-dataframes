# Transformations

Transformations are functions that alter your data, ensuring it is free of sensitive information.

Cape Dataframes has five built-in transformation functions. This document describes what they do, and provides an example of how to use each transformation in your policy.

## Date perturbation

The `date-perturbation` transformation adds random noise to dates. The amount of noise depends on the `min` and `max` values that you set in the policy.

``` yaml
- transform:
    type: date-pertubation
    frequency: <one of: 'year', 'month', 'day', 'hour', 'minute', 'second'>
    min: <int or float>
    max: <int or float>
    # Optional. The base number to initialize the random number generator.
    # Pandas only (Spark does not currently support seeding)
    seed: <int>
```


## Date truncation

The `date-truncation` transformation shortens dates to a unit (year or month). Set the unit in `frequency`.

``` yaml
- transform:
    type: date-truncation
    frequency: <one of: 'year', 'month', 'day', 'hour', 'minute', 'second'>
```

## Numeric pertubation

The `numeric-pertubation` transformation adds random noise to numeric data sets. The amount of noise depends on the `min` and `max` values that you set in the policy.

``` yaml
- transform:
    type: numeric-pertubation
    dtype: <Pandas Series type or Spark Series type>
    min: <int or float>
    max: <int or float>
    # Optional. The base number to initialize the random number generator.
    seed: <int>
```

## Numeric rounding

The `numeric-rounding` transformation rounds numeric values to a given number of decimal places. Use `precision` to set the number of decimal places.

``` yaml
- transform:
    type: numeric-rounding
    dtype: <Pandas Series type or Spark Series type>
    precision: <int>
```

## Tokenizer

The `tokenizer` transformation maps a string to a token to obfuscate it.

!!! warning
    Linkable tokenization for sensitive data is vulnerable to privacy attacks. Cape Privacy does not recommend sharing tokenized data with preserved linkability with untrusted or outside parties. Cape Python does not support anonymized transformations.

``` yaml
- transform:
    type: tokenizer
    # Default is 64
    max_token_len: <int or bytes>
    # If unspecified, Cape Dataframes uses a random byte string
    key: <string or byte string>
```

## ReversibleTokenizer

The `ReversibleTokenizer` transformation maps a sting to a token to obfuscate it. However, when using the `ReversibleTokenizer`, the tokens can be reverted back to their plaintext form by using the `TokenReverser`.

```yaml
- transform:
    type: reversible-tokenizer
    # If unspecified, Cape Dataframes uses a random byte string
    key: <string or byte string>
```

## TokenReverser

The `TokenReverser` is designed to be used with the `ReversibleTokenizer`. The `TokenReverser` reverts tokens produced by the `ReversibleTokenizer` back to their plaintext form.

```yaml
- transform:
    type: token-reverser
    # If unspecified, Cape Dataframes uses a random byte string
    key: <string or byte string>
```
