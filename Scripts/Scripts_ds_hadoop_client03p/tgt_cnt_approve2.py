import datetime
from datetime import date, timedelta
import sys

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt update started').format(datetime.datetime.now())

HIVE_HOST = 'ds-hadoop-cs01p'
HIVE_PORT = 10000
HIVE_USER = 'kposminin'
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

action_type_to_load = ['tinkoff_platinum_complete_application']

query_pattern = '''
WITH t AS 
 (
   SELECT
   v.url_fragment AS urlfr
   ,count(distinct if(ta.ymd between v.ymd and date_add(v.ymd,3),ta.id,Null)) as cnt_positive
   ,count(distinct v.id) as cnt_total
  FROM
   prod_odd.visit_feature v
   left join (
     SELECT DISTINCT a.id,a.ymd 
     FROM prod_features_liveinternet.user_action a 
     WHERE a.ymd between date_add('#ymd', -30) and date_add('#ymd',3) 
       and a.action_type = '#action_type'
   ) ta on v.id = ta.id
  WHERE
   v.ymd between date_add('#ymd', -30) and '#ymd'
   and v.load_src = 'LI.02'
  GROUP BY 
   v.url_fragment
   ) 
INSERT OVERWRITE TABLE 
  user_kposminin.urlfr_tgt_cnt PARTITION (ymd='#ymd', target='#action_type03_1m') 
SELECT 
 urlfr AS urlfr
 ,nvl(cnt_positive, 0) as cnt_positive
 ,cnt_total
 ,log((cnt_positive + 0.1)/(cnt_total - cnt_positive + 0.1)) as score
FROM t
WHERE
 (cnt_total > 10000 or cnt_positive > 0) 
'''


for ymd in sys.argv[1:]:
    for at in action_type_to_load:
        q = query_pattern.replace('#action_type', at).replace('#ymd',ymd)
        print(q)
        cursor.execute(q)
        print('{} - {} updated.'.format(ymd_start,ymd_end))




