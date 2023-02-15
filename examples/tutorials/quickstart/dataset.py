# SKIP_CI

import pandas as pd


def load_dataset(sess=None):
    dataset = pd.DataFrame(
        {
            "name": ["alice", "bob"],
            "age": [34, 55],
            "birthdate": [pd.Timestamp(1985, 2, 23), pd.Timestamp(1963, 5, 10)],
            "salary": [59234.32, 49324.53],
            "ssn": ["343554334", "656564664"],
        }
    )
    if sess is not None:
        return sess.createDataFrame(dataset)
    else:
        return dataset
