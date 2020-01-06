import glob
import logging
import tempfile
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def quiet_py4j(level=logging.WARN):
    """
    Reduce the amount of trace info produced by Spark's runtime
    In Spark 2.x we can configure it dynamically:
    `spark.sparkContext.setLogLevel('WARN')`
    """
    logging.getLogger('py4j').setLevel(level)


def _external_libraries(path):
    """
    Returns a comma separated list of all of the *.jar packages found in 'path'
    """
    jars = ",".join(glob.glob("external-libraries/*.jar"))

    logger.debug('External jars:')
    for jar in jars:
        logger.debug(jar)

    return jars


def _default_spark_configuration():
    """
    Default configuration for spark's unit testing
    All of the external libraries (jars) area also made available to the spark application (set in 'spark.jars')
    """
    external_jars = _external_libraries('')

    conf = SparkConf()

    if external_jars:
        conf.set("spark.jars", external_jars)

    d = tempfile.TemporaryDirectory()
    derby_d = d.name if os.name != 'nt' else "/" + d.name.replace("\\", "/")

    # Basic spark configuration
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    conf.set("spark.app.name", "amaroq-core-test-app")
    conf.set("spark.worker.cleanup.enabled", "true")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.sql.hive.metastorePartitionPruning", "true")
    # Hive specification for ORC and dynamic partitioning
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("hive.stats.fetch.partition.stats", "true")
    conf.set("hive.compute.query.using.stats", "true")
    conf.set("hive.stats.autogather", "true")
    # Define warehouse for local execution
    conf.set('spark.driver.extraJavaOptions', '-Dderby.system.home=' + derby_d)
    conf.set('spark.sql.warehouse.dir', d.name)
    conf.set('derby.stream.error.file', d.name)

    return conf


def bootstrap_test_spark_session(conf=None):
    """
    Setup SparkContext
    """
    conf = conf or _default_spark_configuration()
    ss = SparkSession.builder.master("local").config(conf=conf).enableHiveSupport().getOrCreate()
    quiet_py4j()

    return ss


ss = bootstrap_test_spark_session()
