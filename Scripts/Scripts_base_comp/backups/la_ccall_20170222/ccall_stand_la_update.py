
# coding: utf-8

import datetime,time
import os
import re

print('{0}: prod_lookalike.stand_la_ccall update started.').format(datetime.datetime.now())

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

segment_nm = 'la_stand_ccall'
clean_after_self = True

def get_last_visits_r_date(cursor):
    cursor.execute('select max(ymd) from prod_features_liveinternet.visits')
    return cursor.fetchone()[0]

def get_last_model_calc_date(cursor):
    cursor.execute('select max(ymd) from prod_lookalike.phone_x_segment where segment_nm = "#segment_nm"'.replace('#segment_nm', segment_nm))
    return cursor.fetchone()[0]

ymd_loaded     = get_last_visits_r_date(cursor)
ymd_calculated = get_last_model_calc_date(cursor)


cnt = 0
while ymd_calculated >= ymd_loaded:    
    cnt += 1
    if cnt > 70:
        print('{}. Failed to wait for visits to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd_loaded) + '*'*60)
        exit(1)
    time.sleep(1000)
    ymd_loaded = get_last_visits_r_date(cursor)

print('{} Got visits for {}. Calculating signals later than {}'.format(datetime.datetime.now(),ymd_loaded,ymd_calculated))
ind = ymd_loaded.replace('-','')

cursor.execute('select max(ymd) from prod_features_liveinternet.urlfr_tgt_cnt_cumulative2')
tgt_date = cursor.fetchone()[0]

queries = '''

drop table if exists prod_lookalike.#segment_nm_#ind;

create table prod_lookalike.#segment_nm_#ind as
select
    b.contact_str as phone
   ,count(distinct v.id) as id_cnt
   ,max(t.score) as score
   ,'#segment_nm' as segment_nm
   ,v.ymd
from
    prod_features_liveinternet.visits v
    left join (
     select urlfr, score
     from prod_features_liveinternet.urlfr_tgt_cnt_cumulative2
     where target  = 'tinkoff_platinum_complete_application@tinkoff_action'      
       and ymd = '#tgt_date'
       and (cnt_total > 300000 or cnt_positive > 30)
    ) t on t.urlfr = v.urlfr
    inner join (select uid_str,h_uid_rk from prod_dds.h_uid where load_src = 'LI.02') d on d.uid_str = v.id
    inner join (select h_contact_rk, h_uid_rk from prod_dds.l_uid_contact where contact_type_cd = 'PHONE') c on d.h_uid_rk = c.h_uid_rk
    inner join (select h_contact_rk, contact_str from prod_dds.h_contact where contact_type_cd = 'PHONE') b on b.h_contact_rk = c.h_contact_rk  
where 
  v.ymd > '#ymd_calculated'
  group by b.contact_str, v.ymd
;

insert into prod_lookalike.phone_x_segment partition (segment_nm, ymd)
select
    a.phone as phone_num
   ,a.score
   ,current_timestamp() as load_dttm
   ,a.segment_nm as segment_nm
   ,a.ymd as ymd
from
  prod_lookalike.#segment_nm_#ind a
  left join prod_lookalike.threshold th on th.segment_nm = a.segment_nm
where
  a.score >= nvl(th.threshold, -999999)
;
'''.replace('#tgt_date',tgt_date).replace('#ymd_calculated',ymd_calculated).replace('#ind',ind).replace('#segment_nm',segment_nm)

#print(queries)

for q in queries.split(';'):
    if re.search('[^ \n\t]',q):
        #print(q)
        cursor.execute(q)

if (clean_after_self):
    for q in queries.split(';'):
        if 'drop table'in q:
            cursor.execute(q)

os.popen('touch /home/bigdatasys/projects/la_ccall/#segment_nm_ready'.replace('#segment_nm',segment_nm))
print('{0}: prod_lookalike.stand_la_ccall update successfully finished.').format(datetime.datetime.now())

