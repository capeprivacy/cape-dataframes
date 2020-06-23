from pyspark import sql


def make_session(name, arrow=True):
    sess_builder = sql.SparkSession.builder
    sess_builder = sess_builder.appName(name)
    sess = sess_builder.getOrCreate()
    if arrow:
        sess.conf.set("spark.sql.execution.arrow.enabled", "true")
    return sess
