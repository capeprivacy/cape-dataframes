from pyspark import sql


def make_session(name):
    sess_builder = sql.SparkSession.builder
    sess_build = sess_builder.appName(name)
    sess = sess_builder.getOrCreate()
    return sess
