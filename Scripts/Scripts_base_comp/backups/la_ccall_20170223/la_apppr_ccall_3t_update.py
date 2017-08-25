# coding: utf-8

#Calculating segment signals NOT present in main segment

import datetime,time
import os
import re


segment_nm = 'la_apppr_ccall_3t'
main_seg = 'la_apppr_ccall_2'
share_to_take = 0.7 # Only share of relevant segment signals will be taken.

#No raw data generated so no need for clean
#clean_after_self = True
#days_to_keep = 5 # Only relevant if clean_after_self = True

print('{0}: prod_lookalike.{1} update started.').format(datetime.datetime.now(),segment_nm)

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

def get_last_raw_data_date(cursor):
    cursor.execute('select max(ymd) from prod_lookalike.#main_seg_raw'.replace('#main_seg', main_seg))
    return cursor.fetchone()[0]

def get_last_model_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-02-12") from prod_lookalike.phone_x_segment where segment_nm = "#segment_nm"'.replace('#segment_nm', segment_nm))
    return cursor.fetchone()[0]

def get_last_main_seg_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-02-12") from prod_lookalike.phone_x_segment where segment_nm = "#main_seg"'.replace('#main_seg', main_seg))
    return cursor.fetchone()[0]

ymd_loaded     = get_last_raw_data_date(cursor)
ymd_calculated = get_last_model_calc_date(cursor)
ymd_main_seg   = get_last_main_seg_calc_date(cursor)


cnt = 0
while (ymd_calculated >= ymd_loaded) | (ymd_calculated >= ymd_main_seg):  
    cnt += 1
    if cnt > 70:
        print('{}. Failed to wait for visits to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd_loaded) + '*'*60)
        exit(1)
    print('{}.Waiting for visits later than {}. Sleeping.'.format(datetime.datetime.now(),ymd_loaded))
    time.sleep(1000)
    ymd_loaded = get_last_raw_data_date(cursor)

print('{} Got raw data for {}. Calculating signals later than {}'.format(datetime.datetime.now(),ymd_loaded,ymd_calculated))
ind = ymd_loaded.replace('-','')

queries = ('''


insert into prod_lookalike.phone_x_segment partition (segment_nm, ymd)
select
    a.phone_num
   ,a.score
   ,current_timestamp() as load_dttm
   ,'#segment_nm' as segment_nm
   ,a.ymd as ymd
from
  prod_lookalike.#main_seg_raw a
  left join prod_lookalike.threshold th on th.segment_nm = '#segment_nm'
  left join prod_lookalike.phone_x_segment m on a.phone_num = m.phone_num and a.ymd = m.ymd and m.segment_nm = '#main_seg'
where
  a.score >= nvl(th.threshold, -999999)
  and a.ymd > '#ymd_calculated'
  and (m.phone_num is Null)
  and rand() < #share_to_take
;
'''.replace('#ymd_calculated',ymd_calculated)   
   .replace('#segment_nm',segment_nm)
   .replace('#main_seg',main_seg)
   .replace('#share_to_take', str(share_to_take))

)

#print(queries)

for q in queries.split(';'):
    if re.search('[^ \n\t]',q):
        print(q)
        cursor.execute(q)
    
os.popen('touch /home/bigdatasys/projects/la_ccall/#segment_nm_#ind_ready'.replace('#segment_nm',segment_nm).replace('#ind',ind)).read()

print('{0}: prod_lookalike.{1} update successfully finished.').format(datetime.datetime.now(),segment_nm)
