import os
import sys
import subprocess

#spark_home = '/opt/apache/spark'
#sys.path.append(spark_home + '/python')
#sys.path.append(spark_home + '/python/lib/py4j-0.8.2.1-src.zip')

spark_home = '/opt/apache/spark-1.6.0-bin-hadoop2.6'
sys.path.append(spark_home + '/python')
sys.path.append(spark_home + '/python/lib/py4j-0.9-src.zip')

os.environ['SPARK_HOME'] = spark_home
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['HIVE_CONF_DIR'] = '/etc/hive/conf'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['HADOOP_YARN_HOME'] = '/opt/cloudera/parcels/CDH/lib/hadoop-yarn'
os.environ['HADOOP_HOME'] = '/opt/cloudera/parcels/CDH/lib/hadoop'
os.environ['PYSPARK_PYTHON'] = '/opt/anaconda/bin/python'
#os.environ['SPARK_CLASSPATH'] = '/etc/hive/conf.cloudera.hive1'
os.environ['SPARK_DIST_CLASSPATH'] = subprocess.check_output('hadoop classpath', shell = True)
os.environ['SPARK_MASTER_PORT'] = "4202"
os.environ['SPARK_WORKER_PORT'] = "4203"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master yarn --deploy-mode client --driver-memory 8g --num-executors 4  --executor-memory 8g --executor-cores 1 --conf spark.yarn.executor.memoryOverhead=2048 --conf spark.driver.maxResultSize=4g --conf spark.yarn.queue=kposminin pyspark-shell'

sys.path.append('/home/k.p.osminin/.ipython/scripts')
import pyspark
import ml_tools

try:
    sc.stop()
except:
    pass
from pyspark import SparkContext
from pyspark import HiveContext

from cluster_setup import init_cluster
sc, hc = init_cluster('ccr_apply')
