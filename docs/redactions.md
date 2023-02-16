# Redactions

Redactions involve dropping the matched data. Unlike [transformations](./transformations), which modify but preserve data, redactions will change the shape of your dataframes.

Cape Dataframes has one built-in redaction function. This document describes what it does, and provides an example of how to use it in your policy.

!!! warning
    Redactions change the shape of your data.

## Column redaction

The `column-redact` redaction deletes matching columns.

```yaml
- transform:
    type: "column-redact"
    # Replace <COLUMN_NAME> with the column name you want to redact.
    columns: ["<COLUMN_NAME>"]
```

