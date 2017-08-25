mport datetime
from datetime import date, timedelta

print('{0}: Start.').format(datetime.datetime.now())

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


query_old_table = '''

create table user_kposminin.tmp1 as
select
       v.ymd
     , v.id
     , max(if(p.id is Null,0,1)) as label
     , max(if(p.first_day = 1, 1, 0)) as first_day
     , max(score) as max_tcs_score
     , avg(t.score) as avg_tcs_score
     , sum(t.score) as sum_tcs_score
     , min(t.score) as min_tcs_score
  from prod_features_liveinternet.visits v
  left join (  
       select id, if(ymd = '2016-10-26',1,0) as first_day from prod_features_liveinternet.user_action 
        where ymd between '2016-10-26' and '2016-10-28'
          and action_type = 'tinkoff_platinum_complete_application'
     ) p   on p.id = v.id
  left join (
       select urlfr
            , log((cnt_positive + 0.1)/(cnt_total - cnt_positive + 0.1)) as score
            , cnt_positive
            , cnt_total
         from prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 a
       where target  = 'tinkoff_platinum_complete_application@tinkoff_action'  
         and (cnt_total > 300000 or cnt_positive > 30)
         and ymd = '2016-10-24'
     ) t on t.urlfr = v.urlfr
 where 
       v.ymd = '2016-10-25'
 group by 
       v.id
     , v.ymd

'''

query_new_table = '''

create table user_kposminin.tmp1 as
select
       v.ymd
     , v.id
     , max(if(p.id is Null,0,1)) as label
     , max(if(p.first_day = 1, 1, 0)) as first_day
     , max(score) as max_tcs_score
     , avg(t.score) as avg_tcs_score
     , sum(t.score) as sum_tcs_score
     , min(t.score) as min_tcs_score
  from prod_features_liveinternet.visits_rorc v
  left join (  
       select id, if(ymd = '2016-10-26',1,0) as first_day from prod_features_liveinternet.user_action 
        where ymd between '2016-10-26' and '2016-10-28'
          and action_type = 'tinkoff_platinum_complete_application'
     ) p   on p.id = v.id
  left join (
       select urlfr
            , log((cnt_positive + 0.1)/(cnt_total - cnt_positive + 0.1)) as score
            , cnt_positive
            , cnt_total
         from prod_features_liveinternet.urlfr_tgt_cnt_cumulative2 a
       where target  = 'tinkoff_platinum_complete_application@tinkoff_action'  
         and (cnt_total > 300000 or cnt_positive > 30)
         and ymd = '2016-10-24'
     ) t on t.urlfr = v.urlfr
 where 
       v.ymd = '2016-10-25'
 group by 
       v.id
     , v.ymd

'''

start = datetime.datetime.now()
cursor.execute(query_old_table)
print('{}. Old query work time: {}.'.format(datetime.datetime.now(),datetime.datetime.now() - start))

start = datetime.datetime.now()
cursor.execute(query_new_table)
print('{}. New query work time: {}.'.format(datetime.datetime.now(),datetime.datetime.now() - start))


