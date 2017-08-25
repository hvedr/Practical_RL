# coding: utf-8

# Transfer segments data between prod_ex_machina.user_segment and prod_lookalike.phone_x_segment for yesterday.


import datetime,time
import os
import re

#constants
#segments to transfer
segs = "'la_apppr_ccall_2_1','la_stand_ccall_2','lala_apppr_1','la_apppr_ccall_2_2','la_apppr_ccall_1_2'"

#init connection
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


def get_yesterday(cursor):
    cursor.execute('select date_add(current_date, -1)')
    return cursor.fetchone()[0]

def get_last_model_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-01-01") from prod_ex_machina.user_segment where segment_nm in (#segs)'
                     .replace('#segs', segs)
                  )
    return cursor.fetchone()[0]

ymd = get_yesterday(cursor) 
ymd_loaded = get_last_model_calc_date(cursor) 

cnt = 0
while ymd_loaded < ymd:    
    cnt += 1
    if cnt > 70:
        print('{}. Failed to wait for segments to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd) + '*'*60)
        exit(1)
    print('Sleeping')
    time.sleep(1000)
    ymd_loaded = get_last_model_calc_date(cursor) 

print('{} Got segments for {}. Transferring signals for {}'.format(datetime.datetime.now(),ymd_loaded,ymd))
#ind = ymd.replace('-','')


queries = '''

SET hive.exec.dynamic.partition.mode = non-strict;

insert overwrite table prod_lookalike.phone_x_segment partition (segment_nm,  ymd)
select 
  wu_rk as phone_num, 
  probability as score,
  current_timestamp() as load_dttm,
  segment_nm,
  ymd
from prod_ex_machina.user_segment 
where 
  segment_nm in (#segs)
  and ymd = '#ymd'
;

'''.replace('#ymd',ymd).replace('#segs', segs)


for q in queries.split(';'):
    if re.search('[^ \n\t]',q):
        #print(q)
        cursor.execute(q)

for seg in segs.split(','):
    os.popen('touch /home/bigdatasys/projects/la_ccall/#segment_nm_#ind_ready'
             .replace('#segment_nm', seg.replace("'","") )
             .replace('#ind',ymd.replace('-',''))
        ).read()
   
print('{}. Segments {} for {} successfully transferred.'.format(datetime.datetime.now(), segs, ymd))
