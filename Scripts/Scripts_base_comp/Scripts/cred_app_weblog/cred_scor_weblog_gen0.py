
# coding: utf-8

import datetime,time
import os, sys
import re

print('{0}: Calc cred scoring weblog.').format(datetime.datetime.now())

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

oneshot_queries = '''
create table user_kposminin.aza_cred_scoring_ph_id as 
select 
  m.phone_num,
  m.load_src,
  m.id,
  to_date(a.create_dt) as call_ymd
from
  user_kposminin.aza_to_liru_risk_l170131 a
  inner join (
     select 
       uid_str as id,
       property_value as phone_num,
       load_src
     from
       prod_dds.md_uid_property 
     where
       property_cd = 'PHONE'
    ) m on substr(m.phone_num,3,15) = substr(a.phone_mobile, 2, 15)
;


create table user_kposminin.cred_scoring_weblog (
  phone_num string,
  call_ymd  string,
  url       string,
  cnt          int,
  min_time     int,
  max_time     int  
  )
  partitioned by (ymd string)
  stored as RCFile;


'''

query_pattern = '''
-- Calc cred scoring weblog for #ymd.
insert overwrite table user_kposminin.cred_scoring_weblog partition (ymd)
select
  a.phone_num,
  a.call_ymd,
  wl.url,
  count(*) as cnt,
  min(60 * hour(wl.event_dttm) + minute(wl.event_dttm)) as min_time,
  max(60 * hour(wl.event_dttm) + minute(wl.event_dttm)) as max_time,
  wl.ymd
from
  user_kposminin.aza_cred_scoring_ph_id a
  inner join prod_odd.weblog wl on wl.load_src = a.load_src and wl.uid = a.id
where
  wl.ymd = '#ymd'
  and call_ymd between date_add('#ymd', 1) and date_add('#ymd', 180)
group by
  a.phone_num,
  a.call_ymd,
  wl.url,
  wl.ymd


'''

#Main

calced_dates = ['2016-12-31', '2016-12-30', '2016-12-29', '2016-12-28', '2016-12-27', '2016-12-26', '2016-12-25', '2016-12-24', '2016-12-23', '2016-12-22', '2016-12-21',
 '2016-12-20', '2016-12-19', '2016-12-18', '2016-12-17', '2016-12-16', '2016-12-15', '2016-12-14', '2016-12-13', '2016-11-06', '2016-11-05', '2016-11-04', '2016-11-03',
 '2016-11-02', '2016-11-01', '2016-10-31', '2016-10-30', '2016-10-29', '2016-10-28', '2016-10-27', '2016-10-26', '2016-10-25', '2016-10-24', '2016-10-23', '2016-10-22',
 '2016-10-21', '2016-10-20', '2016-10-19', '2016-10-18', '2016-09-13', '2016-09-12', '2016-09-11', '2016-09-10', '2016-09-09', '2016-09-08', '2016-09-07', '2016-09-06',
 '2016-09-05', '2016-09-04', '2016-09-03', '2016-09-02', '2016-09-01', '2016-08-31', '2016-08-30', '2016-08-29', '2016-08-28', '2016-08-27', '2016-08-26', '2016-07-21',
 '2016-07-20', '2016-07-19', '2016-07-18', '2016-07-17', '2016-07-16', '2016-07-15', '2016-07-14', '2016-07-13', '2016-07-12', '2016-07-11', '2016-07-10', '2016-07-09',
 '2016-07-08', '2016-07-07', '2016-07-06', '2016-07-05', '2016-07-04', '2016-05-28', '2016-05-27', '2016-05-26', '2016-05-25', '2016-05-24', '2016-05-23', '2016-05-22',
 '2016-05-21', '2016-05-20', '2016-05-19', '2016-05-18', '2016-05-17', '2016-05-16', '2016-05-15', '2016-05-14', '2016-05-13', '2016-05-12', '2016-04-04', '2016-04-03',
 '2016-04-02', '2016-04-01', '2016-03-31', '2016-03-30', '2016-03-29', '2016-03-28', '2016-03-27', '2016-03-26', '2016-03-25', '2016-03-24', '2016-03-23', '2016-03-22',
 '2016-02-10', '2016-02-09', '2016-02-08', '2016-02-07', '2016-02-06', '2016-02-05', '2016-02-04', '2016-02-03', '2016-02-02', '2016-02-01', '2016-01-31', '2016-01-30',
 '2016-01-29', '2015-12-18', '2015-12-17', '2015-12-16', '2015-12-15', '2015-12-14', '2015-12-13', '2015-12-12', '2015-12-11', '2015-12-10', '2015-12-09', '2015-12-08',
 '2015-12-07', '2015-12-06', '2015-12-05', '2015-10-25', '2015-10-24', '2015-10-23', '2015-10-22', '2015-10-21', '2015-10-20', '2015-10-19', '2015-10-18', '2015-10-17',
 '2015-10-16', '2015-10-15', '2015-10-14', '2015-10-13', '2015-10-12', '2015-10-11', '2015-10-10', '2015-10-09', '2015-10-08', '2015-10-07', '2015-09-01', '2015-08-31',
 '2015-08-30', '2015-08-29', '2015-08-28', '2015-08-27', '2015-08-26', '2015-08-25', '2015-08-24', '2015-08-23', '2015-08-22', '2015-08-21', '2015-08-20', '2015-08-19',
 '2015-08-18', '2015-08-17', '2015-08-16', '2015-08-15', '2015-08-14', '2015-08-13', '2015-08-12', '2015-08-11', '2015-08-10', '2015-08-09', '2015-08-08', '2015-08-07',
 '2015-08-06', '2015-08-05', '2015-08-04', '2015-08-03', '2015-08-02', '2015-08-01', '2015-07-31', '2015-07-30', '2015-07-29', '2015-07-28', '2015-07-27', '2015-07-26',
 '2015-07-25', '2015-07-24', '2015-07-23', '2015-07-22', '2015-07-21', '2015-07-20', '2015-07-19', '2015-07-18', '2015-07-17', '2015-07-16', '2015-07-15', '2015-07-14',
 '2015-07-13', '2015-07-12', '2015-07-11', '2015-07-10']


#execute one-shot queries

batch = 0
batch_num = 5

ymd = datetime.date(2016,12,31) + datetime.timedelta(days = - (360 + 180) * batch / batch_num)


for _ in range((360+180) / batch_num):
    ymd += datetime.timedelta(days = -1)
    ymd_str = ymd.strftime('%Y-%m-%d')
    if not ymd_str in calced_dates:
        print('{}. Handling {}'.format(datetime.datetime.now(),ymd_str))
        sys.stdout.flush()
        cursor.execute(query_pattern.replace('#ymd', ymd_str))

print('The end')

