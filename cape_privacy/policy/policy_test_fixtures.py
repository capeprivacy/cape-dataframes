y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 1
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 2
    """

named_y = """
    label: test_policy
    transformations:
      - name: plusOne
        type: plusN
        args:
          n:
            value: 1
      - name: plusTwo
        type: plusN
        args:
          n:
            value: 2
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  named: plusOne
                - field: test
                  named: plusTwo
    """

named_not_found_y = lambda saved_tfm, ref_tfm, tfm_type: """
    label: test_policy
    transformations:
      - name: {saved}
        type: {type}
        args:
          n:
            value: 1
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: test
                  named: {ref}
""".format(
    saved=saved_tfm, type=tfm_type, ref=ref_tfm
)

complex_y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              transformations:
                - field: val-int
                  function: numeric-perturbation
                  args:
                    dtype:
                      value: Integer
                    min:
                      value: -10
                    max:
                      value: 10
                    seed:
                      value: 4984
                - field: val-float
                  function: numeric-rounding
                  args:
                    dtype:
                      value: Double
                    precision:
                      value: 1
                - field: name
                  function: tokenizer
                  args:
                    key:
                      value: secret_key
                - field: date
                  function: date-truncation
                  args:
                    frequency:
                      value: year
    """


redact_y = """
    label: test_policy
    spec:
        version: 1
        label: test_policy
        rules:
            - target: records:transactions.transactions
              action: read
              effect: allow
              redact:
                - apple
              where: test > 2
              transformations:
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 1
                - field: test
                  function: plusN
                  args:
                    n:
                      value: 2
    """
