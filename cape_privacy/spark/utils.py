import pyspark
from packaging import version
from pyspark import sql

_3_0_0_VERSION = version.Version("3.0.0")
_spark_version = version.parse(pyspark.__version__)


def configure_session(sess: sql.SparkSession, arrow=True):
    if arrow:
        if _spark_version >= _3_0_0_VERSION:
            sess.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        else:
            sess.conf.set("spark.sql.execution.arrow.enabled", "true")
    return sess


def make_session(name: str, arrow: bool = True):
    sess_builder = sql.SparkSession.builder
    sess_builder = sess_builder.appName(name)
    sess = sess_builder.getOrCreate()
    sess = configure_session(sess, arrow=arrow)
    return sess
