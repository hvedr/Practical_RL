# coding: utf-8

import datetime,time
import os, sys
import re

print('{0}: Calc cred scoring debug visits.').format(datetime.datetime.now())

#init connection
HIVE_HOST = 'ds-hadoop-cs01p'
HIVE_PORT = 10000
HIVE_USER = 'k.p.osminin'
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
    ,'mapreduce.map.failures.maxpercent':'10'
          }

from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()


query_pattern = '''

insert overwrite 
 table user_kposminin.visits_cred_scor_calc_debug_20170429 partition(ymd)
select
  a.phone_mobile
 ,a.ymd as call_ymd
 ,v.id
 ,v.url_fragment as urlfr
 ,v.visit_count as cnt
 ,v.duration_sec as duration
 ,v.average_visit_hour as avg_hour
 ,v.ymd
 
from
  user_kposminin.cred_scor_calc_debug_id_20170429 a  
  inner join prod_odd.visit_feature v on v.id = a.id and v.load_src = a.load_src
where
  v.ymd = '#visit_ymd'
;
'''

def exec_queries(cursor,queries_str):
    '''
       Execute queries using cursor. Queries are in a string separated by ; symbol
    '''
    for q in queries_str.split(';'):
        if re.search('[^ \t\n]',q):
            cursor.execute(q)

def main():

    day = datetime.datetime(2017,04,29)

    #query = ''

    for _ in range(65):
        day = day + datetime.timedelta(days = -1)
        query = query_pattern.replace('#visit_ymd',day.strftime('%Y-%m-%d'))
        print('{}. Executing:\n{}'.format(datetime.datetime.now(),query))
        sys.stdout.flush()
        exec_queries(cursor,query)
    
    #acc_upd_query += accumulator_update_pattern.replace('#ymd',day.strftime('%Y-%m-%d')).replace('#ind',day.strftime('%Y%m%d'))
    
    print('End')

main()