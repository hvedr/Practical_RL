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

ymd_start = str(datetime.date.today()-datetime.timedelta(days=37))
ymd_end = str(datetime.date.today()-datetime.timedelta(days=7))

targets_expr = '''
  'tinkoff_platinum_complete_application@tinkoff_action',
  'tinkoff_platinum_complete_application03@tinkoff_action',
  'tinkoff_platinum_approved_application03@tinkoff_action',
  'all_airlines_complete_application@tinkoff_action',
  'all_airlines_complete_application03@tinkoff_action'
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
 ,target
from
 prod_features_liveinternet.urlfr_tgt_cnt
where 
 ymd between '#ymd_start' and '#ymd_end'
 and target in (#targets_expr)
group by 
 urlfr
 ,target 
'''.replace('#ymd_start',ymd_start).replace('#ymd_end',ymd_end).replace('#targets_expr', targets_expr)

cursor.execute(query)

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 successfully updated. Period: ' + ymd_start + ' --- ' + ymd_end + ' Segments: ' + targets_expr)\
.format(datetime.datetime.now())