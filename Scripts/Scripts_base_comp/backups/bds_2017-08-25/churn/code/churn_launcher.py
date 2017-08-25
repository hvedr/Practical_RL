execfile('/home/bigdatasys/.ipython/profile_spark/startup/00-pyspark-setup.py')

import sys
import os
from time import sleep
import datetime
from datetime import date, timedelta
import subprocess
from pyspark import HiveContext, SparkContext
from subprocess import call
import pandas as pd
import pickle
import glob
import sklearn.datasets as skd
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyhive import hive

HIVE_HOST = 'ds-hadoop-cs01p'
HIVE_PORT = 10000
HIVE_USER = 'bigdatasys'
CONF={'hive.vectorized.execution.enabled':'true'
    ,'mapreduce.map.memory.mb':'4096'
    ,'mapreduce.map.child.java.opts':'-Xmx4g'
    ,'mapreduce.task.io.sort.mb':'1024'
    ,'mapreduce.reduce.child.java.opts':'-Xmx4g'
    ,'mapreduce.reduce.memory.mb':'7000'
    ,'mapreduce.reduce.shuffle.input.buffer.percent':'0.5'
    ,'mapreduce.input.fileinputformat.split.minsize':'536870912'
    ,'mapreduce.input.fileinputformat.split.maxsize':'1073741824'
    ,'hive.optimize.ppd':'true'
    ,'hive.merge.smallfiles.avgsize':'536870912'
    ,'hive.merge.mapredfiles':'true'
    ,'hive.merge.mapfiles':'true'
    ,'hive.hadoop.supports.splittable.combineinputformat':'true'
    ,'hive.exec.reducers.bytes.per.reducer':'536870912'
    ,'hive.exec.parallel':'true'
    ,'hive.exec.max.created.files':'10000000'
    ,'hive.exec.compress.output':'true'
    ,'hive.exec.dynamic.partition.mode':'nonstrict'
    ,'hive.exec.max.dynamic.partitions':'1000000'
    ,'hive.exec.max.dynamic.partitions.pernode':'100000'
    ,'io.seqfile.compression.type':'BLOCK'
    ,'mapreduce.map.failures.maxpercent':'5'
    ,'hive.default.fileformat':'rcfile'
          }

project_dir = '/home/bigdatasys/projects/churn'
code_dir = project_dir + '/code'
static_data_home = '/home/bigdatasys/projects/churn/data/static/'
libsvm_file_path = '/user/hive/warehouse/prod_churn.db/scoring_libsvm_files'
src_tables_locations = '''/prod_dds/bank_card_chain
/prod_dds/bank_card_plastic
/prod_dds/credit_card_account
/prod_dds/customer_x_financial_account
/prod_dds/credit_card_transaction
/prod_dds/individual_customer
/prod_dds/program_product_session
/prod_dds/financial_account_chng
/prod_dds/calls
/prod_emart/credit_card_account
/prod_emart/core_account
/prod_emart/loan_account
/prod_emart/financial_account_application
/prod_emart/customer'''.split('\n')
params = {}
for f in os.listdir(static_data_home):
    params[f] = pickle.load(open(static_data_home + f, 'rb'))
	
ymd = str(date.today())
ymd_to_process = str(date.today() - timedelta(1))


print('*' * 30 + '\nChurn scoring for {0} started at {1}.\n'.format(ymd_to_process, datetime.datetime.now()))
sys.stdout.flush()
src_tables_locations_actual = {x: False for x in src_tables_locations}

all_src_tables_ready = False
#all_src_tables_ready = True


while not all_src_tables_ready:
    for t in src_tables_locations:
        if src_tables_locations_actual[t] == False:          
            listed_files = subprocess.check_output('hadoop fs -ls {0}'.format(t), shell = True)
            listed_files_no_stage = ''.join([line for line in listed_files.split('\n') if 'hive-staging' not in line])
            if ymd in listed_files_no_stage:
                src_tables_locations_actual[t] = True
    all_src_tables_ready = sum(src_tables_locations_actual.values()) == len(src_tables_locations)
    if not all_src_tables_ready:            
        print 'Not all src tables are ready:'
        print src_tables_locations_actual
        print 'Falling asleep for 30 minutes'
        sys.stdout.flush()
        sleep(1800)

sleep(600)

print 'All src tables are ready.\n'
sys.stdout.flush()
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()

with open(code_dir + '/churn_hive_features.sql', 'r') as features_sql:
    features_sql_queries=features_sql.read().replace('YMD_TO_PROCESS', ymd_to_process).split(';')
for query in features_sql_queries:
    print query
    print('Started at {0}.'.format(datetime.datetime.now()))
    sys.stdout.flush()
    cursor.execute(query)
    print('Finished at {0}.'.format(datetime.datetime.now()))
    sys.stdout.flush()

os.environ['PYSPARK_SUBMIT_ARGS']='''--master yarn --deploy-mode client --num-executors 16 --executor-memory 12g 
--executor-cores 1 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.queue=bigdatasys pyspark-shell'''

sc = SparkContext()
hc = HiveContext(sc)

subprocess.call('hadoop fs -rm -r ' + libsvm_file_path, shell = True)
def label_encode_test(c, c_val):
    try:
        res = params['columns_to_encode_encoded_global'][c][c_val]
    except:
        res = params['global_avg']
    return res
def mapper_test(row):
    data = row.asDict()
    for c in params['columns_to_encode']:
        data[c + '_encoded'] = label_encode_test(c, data[c])
    return Row(**data)
def tolibsvm(x):
    i = 0
    res = ''
    for f in x:
        if str(f) != 'nan':
            res += (' ' + str(i) + ':' + str(f))
        i += 1
    res = res[1:]
    return res

df_0 = hc.sql('select * from prod_churn.features where ymd = "{0}"'.format(ymd_to_process)).repartition(40)
df_0 = df_0.fillna(0, subset = params['columns_to_fill_with_zero'])\
.fillna('N', subset = params['columns_to_fill_with_N'])
rdd_0 = df_0.rdd.map(lambda x: mapper_test(x))

df_1 = rdd_0.toDF(sampleRatio=0.05).select([c for c in params['account_features_all']])

def vectorizeData(df):
    return df.rdd.map(lambda x: [x[0], Vectors.dense(x[1:])]).toDF(['account_rk','features'])

vectorized_data = vectorizeData(df_1)

libsvm_formatted = vectorized_data.rdd.map(
    lambda x: [x.account_rk, x.features.toArray().tolist()]
).map(lambda x: [x[0], tolibsvm(x[1])]).cache()

libsvm_formatted.map(lambda x: '0 ' + x[1]).saveAsTextFile(libsvm_file_path)

accounts = libsvm_formatted.map(lambda x: x[0]).collect()
files = glob.glob('/mnt/hdfs_prod/' + libsvm_file_path + "/part*")
data_sk = skd.load_svmlight_files(files, n_features=105, multilabel=False, zero_based='auto')

X = pd.DataFrame(data_sk[0].toarray())
for i in range(2,len(data_sk)):
    if i%2 == 0:
        X = pd.concat([X, pd.DataFrame(data_sk[i].toarray())])

print('Predicting probas {0}.'.format(datetime.datetime.now()))
sys.stdout.flush()
        
pred_prob = params['xgbclassifier'].predict_proba(X)[:,1]

print('Predicting probas done {0}.'.format(datetime.datetime.now()))
sys.stdout.flush()

account_predprob_rdd = sc.parallelize(zip(accounts, [x.item() for x in pred_prob]))
account_predprob_df = account_predprob_rdd.toDF(['account_rk','score'])
hc.registerDataFrameAsTable(account_predprob_df, 'account_predprob_df')
hc.sql('''
insert overwrite table prod_churn.account_x_churn_score partition(ymd = "{0}")
select
 account_rk
 ,score
 ,current_timestamp
from
 account_predprob_df
'''.format(ymd_to_process)
)

print('Scores loaded into prod_churn.account_x_churn_score {0}.'.format(datetime.datetime.now()))
sys.stdout.flush()

sc.stop()