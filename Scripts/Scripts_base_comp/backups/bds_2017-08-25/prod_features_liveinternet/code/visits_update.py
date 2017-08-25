import datetime
from datetime import date, timedelta

print('{0}: prod_features_liveinternet.visits update started').format(datetime.datetime.now())

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

ymd_to_process = str(datetime.date.today()-datetime.timedelta(days=1))

query = '''
insert overwrite 
 table prod_features_liveinternet.visits partition(ymd)
select
 id
 ,urlfr
 ,cast(count(*) as int) cnt
 ,MIN(time) as first_visit
 ,cast(MAX(time) - MIN(time) as int) as duration
 ,cast(from_unixtime(cast(AVG(time) as Bigint), 'HH') AS int) as avg_hour
 ,max(current_timestamp()) as load_dttm
 ,max(ymd) ymd
from
 (
   select
    id
    ,cast(timestamp as Bigint) as time
    ,concat(parse_url(concat('http://', url), 'HOST'), '#', path_fr) as urlfr
    ,ymd
   from
    prod_raw_liveinternet.access_log
    LATERAL VIEW explode(split(parse_url(concat('http://', url), "PATH"), '/')) tt AS path_fr
   where
    ymd = '#ymd'
   ) t
group by
 id
 ,urlfr
'''.replace('#ymd',ymd_to_process)

cursor.execute(query)

print('{0}: prod_features_liveinternet.visits successfully updated. Date: ' + ymd_to_process).format(datetime.datetime.now())
from subprocess import call
call('touch /home/bigdatasys/projects/prod_features_liveinternet/visits_ready', shell = True)

query = '''
insert overwrite 
 table prod_features_liveinternet.urlfr_stat partition(ymd)
select
 urlfr
 ,count(*) visitors
 ,sum(cnt) as hits
 ,current_timestamp() as load_dttm
 ,max(ymd) ymd
from 
 prod_features_liveinternet.visits 
where 
 ymd='#ymd'
group by
 urlfr
'''.replace('#ymd',ymd_to_process)

cursor.execute(query)

print('{0}: prod_features_liveinternet.urlfr_stat successfully updated. Date: ' + ymd_to_process).format(datetime.datetime.now())