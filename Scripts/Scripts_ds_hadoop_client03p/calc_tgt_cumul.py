import datetime
from datetime import date, timedelta

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 update started').format(datetime.datetime.now())

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
          }

from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()


targets_expr = '''
  'tinkoff_platinum_complete_application3@tinkoff_action',
  'tinkoff_platinum_approved_application3@tinkoff_action',
  'all_airlines_complete_application3@tinkoff_action'
'''.replace('\n',' ')

query = '''
INSERT OVERWRITE 
 TABLE prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 PARTITION (ymd, target)
SELECT 
 urlfr
 ,sum(cnt_positive) as cnt_positive
 ,sum(cnt_total) as cnt_total
 ,log((sum(cnt_positive) + 0.1)/(sum(cnt_total) - sum(cnt_positive) + 0.1)) as score
 ,current_timestamp() as load_dttm
 ,'#ymd_end' as ymd
 ,'tinkoff_platinum_approved_application3@tinkoff_action'
from
 prod_features_liveinternet.urlfr_tgt_cnt
where 
 ymd between '#ymd_start' and '#ymd_end'
 and target in ('tinkoff_platinum_approved_application3@tinkoff_action','tinkoff_platinum_approved3_application@tinkoff_action')
group by 
 urlfr
'''

for d in range(0,44,14):
    ymd_start = datetime.date(2016,10,23)  + datetime.timedelta(days=d)
    ymd_end = ymd_start  + datetime.timedelta(days=30)
    cursor.execute(query.replace('#ymd_start',str(ymd_start)).replace('#ymd_end',str(ymd_end)))
    print('{} - {} updated.'.format(ymd_start,ymd_end))

print('prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 successfully updated.')