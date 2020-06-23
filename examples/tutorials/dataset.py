import pandas as pd
from pyspark import sql


def load_dataset(framework):
    pdf = pd.DataFrame({
        "name": ["alice", "bob"],
        "age": [34, 55],
        "birthdate": [pd.Timestamp(1985, 2, 23), pd.Timestamp(1963, 5, 10)],
        "salary": [59234.32, 49324.53],
        "ssn": ["343554334", "656564664"],
    })
    if framework == "spark":
        sess = _make_session("cape.test")
        return sess.createDataFrame(pdf)
    else:
        return pdf


def _make_session(name, arrow=True):
    sess_builder = sql.SparkSession.builder
    sess_builder = sess_builder.appName(name)
    sess = sess_builder.getOrCreate()
    if arrow:
        sess.conf.set("spark.sql.execution.arrow.enabled", "true")
    return sess
