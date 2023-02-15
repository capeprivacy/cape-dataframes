# Policies

The data policy defines the data you want to change, and the [transformations](/libraries/cape-dataframes/transformations/) or [redactions](/libraries/cape-dataframes/redactions/) you want to apply.

Cape Dataframes requires data policies in YAML format. This example describes all the available YAML objects:

``` yaml
# Required. The policy name.
label: test_policy
# Required. The Cape Dataframes specification version. Must be 1.
version: 1
# Configure your named transformations.
# Named transformations allow you to reuse a transformation
# with a set value throughout your policy.
transformations:
    # This named transformation uses the built-in tokenizer transformation
    - name: my_tokenizer
      type: tokenizer
      max_token_len: 10
      key: "my secret"
rules:
    # Required. The column name.
    - match: 
        name: fruit
      actions:
        # This example shows a named transformation.
        # It tells the policy runner to apply the my_tokenizer transformation
        # to all fields in the "fruit" column.
        - transform:
            name: my_tokenizer
    - match: 
        name: weight
      actions:
        - transform:
            # This example shows an unnamed transformation.
            # It tells the policy runner to:
            # (1) Apply the transformation numeric-rounding 
            # (2) Round to one decimal place
            type: numeric-rounding
            dtype: Double
            precision: 1
```

