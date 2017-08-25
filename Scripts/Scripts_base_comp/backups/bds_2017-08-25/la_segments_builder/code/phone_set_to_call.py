from os import path
from time import sleep
from subprocess import call

def phone_for_id_ready():
    res = path.exists('/home/bigdatasys/projects/la_segments_builder/phone_for_id_ready')
    return res

while not phone_for_id_ready():
    sleep(60)


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


from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()

import datetime
from datetime import date, timedelta

ymd = str(date.today() - timedelta(1))
ind = ymd.replace('-', '')
print('*' * 30 + '\nphone_set_to_call for {0} started at {1}.\n'.format(ymd, datetime.datetime.now()))

queries = []

queries.append("""
insert overwrite
 table prod_ccall.sme_ip_phones_test partition (ymd)
select distinct
 phone_num
 ,current_timestamp() as load_dttm
 ,ymd
from 
 prod_lookalike.phone_x_segment a
where 
 a.ymd in ("ymd_dt")
 and a.segment_nm in ('sme_life_v0.1')
""".replace('#ind',ind).replace('ymd_dt', ymd)
)

i = 0
for query in queries:
    print query
    print('{1}. Executing query {0}.'.format(i,datetime.datetime.now()))
    i += 1
    cursor.execute(query)


call('rm /home/bigdatasys/projects/la_segments_builder/phone_for_id_ready', shell = True)

print('\nphone_set_to_call for {0} SUCCESS. {1}.\n'.format(ymd, datetime.datetime.now()) + '*' * 30 + '\n')