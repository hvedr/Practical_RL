import datetime
from datetime import date, timedelta

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt update started').format(datetime.datetime.now())

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

ymd_to_process = datetime.date.today() - datetime.timedelta(days=7)
action_type_to_load = ['tinkoff_platinum_complete_application', 'all_airlines_complete_application','tinkoff_platinum_approved_application']

query_pattern = '''
WITH t AS 
 (
   SELECT
   v.urlfr AS urlfr
   ,COUNT(*) AS cnt_positive   
  FROM
   (SELECT DISTINCT a.id FROM prod_features_liveinternet.user_action a WHERE a.ymd between '#ymd_start' and '#ymd_end' and a.action_type = '#action_type') ta 
    inner join prod_features_liveinternet.visits v on v.id = ta.id    
  WHERE
   v.ymd='#ymd0'     
  GROUP BY 
   v.urlfr
   ) 
  
INSERT OVERWRITE TABLE 
 prod_features_liveinternet.urlfr_tgt_cnt PARTITION (ymd='#ymd0', target='#action_type#N@tinkoff_action') 
SELECT 
 s.urlfr AS urlfr
 ,nvl(t.cnt_positive, 0) as cnt_positive
 ,s.visitors as cnt_total
 ,current_timestamp() as load_dttm
FROM
 prod_features_liveinternet.urlfr_stat s 
 LEFT OUTER JOIN t ON t.urlfr = s.urlfr 
WHERE 
 s.ymd='#ymd0'
'''

for at in action_type_to_load:
    if True:
        ymd_to_process = datetime.date.today() - datetime.timedelta(days = 4)
        ymd0      = ymd_to_process.strftime('%Y-%m-%d')
        ymd_start = (ymd_to_process + datetime.timedelta(days = 0)).strftime('%Y-%m-%d')
        ymd_end   = (ymd_to_process + datetime.timedelta(days = 3)).strftime('%Y-%m-%d')
        N = '03'
    q = query_pattern.replace('#action_type', at).replace('#ymd0',ymd0).replace('#ymd_start',ymd_start).replace('#ymd_end',ymd_end).replace('#N', N)
    #print(q)
    cursor.execute(q)

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt successfully updated. Date: {1}').format(datetime.datetime.now(),ymd_to_process)
