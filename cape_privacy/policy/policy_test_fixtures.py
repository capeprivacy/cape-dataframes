y = """
    label: test_policy
    version: 1
    rules:
      - match:
          name: test
        actions:
          - transform:
              type: plusN
              n: 1
          - transform:
              type: plusN
              n: 2
    """

named_y = """
    version: 1
    label: test_policy
    transformations:
      - name: plusOne
        type: plusN
        n: 1
      - name: plusTwo
        type: plusN
        n: 2
    rules:
      - match:
          name: test
        actions:
          - transform:
              name: plusOne
          - transform:
              name: plusTwo
    """

named_with_secret_y = """
    version: 1
    label: test_policy
    transformations:
      - name: plusOne
        type: plusN
        n: 1
      - name: tokenWithSecret
        type: tokenizer
        key:
          type: secret
          name: my-key
          value: BASE
    rules:
      - match:
          name: test
        actions:
          - transform:
              name: plusOne
          - transform:
              name: plusTwo
    """


def named_not_found_y(saved_tfm, ref_tfm, tfm_type):
    return """
        label: test_policy
        version: 1
        transformations:
          - name: {saved}
            type: {type}
            n: 1
        rules:
          - match:
              name: test
            actions:
              - transform:
                  name: {ref}
    """.format(
        saved=saved_tfm, type=tfm_type, ref=ref_tfm
    )


complex_y = """
    label: test_policy
    version: 1
    rules:
      - match:
          name: val-int
        actions:
          - transform:
              type: numeric-perturbation
              dtype: Integer
              min: -10
              max: 10
              seed: 4984
      - match:
          name: val-float
        actions:
          - transform:
              type: numeric-rounding
              dtype: Double
              precision: 1
      - match:
          name: name
        actions:
          - transform:
              type: tokenizer
              key: secret_key
      - match:
          name: date
        actions:
          - transform:
              type: date-truncation
              frequency: year
    """


redact_y = """
    label: test_policy
    version: 1
    rules:
      - match:
          name: apple
        actions:
          - drop
      - match:
          name: test
        actions:
          - transform:
              type: plusN
              n: 1
          - transform:
              type: plusN
              n: 2
    """

secret_yaml = """
label: masking_policy
version: 1
transformations:
  - name: reversible
    type: reversible-tokenizer
    key:
      type: secret
      value: m5YNKBP-a3GMyy52457ok-4zQHqLuiB3aFD7mPTBpoc
  - name: reverse
    type: token-reverser
    key:
      type: secret
      value: m5YNKBP-a3GMyy52457ok-4zQHqLuiB3aFD7mPTBpoc
rules:
  - match:
      name: name
    actions:
      - transform:
          name: reversible
  - match:
      name: name
    actions:
      - transform:
          name: reverse
"""

reversible_yaml = """
label: masking_policy
version: 1
transformations:
  - name: reversible
    type: reversible-tokenizer
    key:
      type: secret
      value: m5YNKBP-a3GMyy52457ok-4zQHqLuiB3aFD7mPTBpoc
rules:
  - match:
      name: name
    actions:
      - transform:
          name: reversible
"""
