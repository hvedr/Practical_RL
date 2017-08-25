
import datetime,time
import os
import re
import sys


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



query_pattern = '''

insert overwrite 
 table user_kposminin.cred_app_visits partition(ymd,financial_product_type_cd)
select
  phone_mobile
 ,max(default_flg) as default_flg
 ,date_add(retro_date, 1) as call_ymd
 ,count(distinct id) as id_cnt
 ,count(distinct load_src) as load_src_cnt
 ,urlfr
 ,cast(count(*) as int) cnt
 ,MIN(time) as first_visit
 ,cast(MAX(time) - MIN(time) as int) as duration
 ,cast(from_unixtime(cast(AVG(time) as Bigint), 'HH') AS int) as avg_hour
 ,max(current_timestamp()) as load_dttm
 ,max(ymd) ymd
 ,max(financial_product_type_cd) as financial_product_type_cd
 
from
 (
   select
     phone_num as phone_mobile
    ,id
    ,financial_product_type_cd
    ,retro_date
    ,load_src
    ,default_flg    
    ,cast(event_dttm as Bigint) as time
    ,concat(url_domain, '#', path_fr) as urlfr
    ,ymd
   from
    (
     select
       a.phone_num, 
       a.id,
       a.financial_product_type_cd, 
       a.retro_date, 
       a.default_flg,
       a.load_src,
       w.event_dttm,
       w.url,
       w.url_domain,
       w.ymd
     from
       user_kposminin.cred_app_id_phone a 
       inner join prod_odd.weblog w on w.uid = a.id and w.load_src = a.load_src
     where
       w.ymd = '#visit_ymd'
       and a.retro_date between date_add(w.ymd, 1) and date_add(w.ymd, 365)
       
    ) tmp
    
    LATERAL VIEW explode(split(parse_url(url, "PATH"), '/')) tt AS path_fr

   ) t
group by
  phone_mobile
 ,date_add(retro_date, 1)
 ,urlfr
 ,phone_mobile
 
'''   


l = ['2015-12-30', '2015-12-29', '2015-12-28','2015-12-27', '2015-12-26', '2015-12-25', '2015-12-24', '2015-12-23', '2015-12-22',
 '2015-12-21', '2015-12-20', '2015-12-19', '2015-12-18', '2015-12-17', '2015-12-16', '2015-12-15', '2015-12-14', '2015-12-13',
 '2015-12-12', '2015-12-11', '2015-12-10', '2015-12-09', '2015-12-08', '2015-12-07', '2015-12-06', '2015-12-05', '2015-12-04',
 '2015-12-03', '2015-12-02', '2015-12-01', '2015-11-30', '2015-11-07', '2015-11-06', '2015-11-05', '2015-11-04', '2015-11-03',
 '2015-11-02', '2015-11-01', '2015-10-31', '2015-10-30', '2015-10-29', '2015-10-28', '2015-10-27', '2015-10-26', '2015-10-25',
 '2015-10-24', '2015-10-23', '2015-10-22', '2015-10-21', '2015-10-15', '2015-10-10', '2015-10-09', '2015-10-08', '2015-10-07',
 '2015-10-06', '2015-10-05', '2015-10-04', '2015-10-03', '2015-10-02', '2015-10-01', '2015-09-30', '2015-09-29', '2015-09-28',
 '2015-09-27', '2015-09-26', '2015-09-25', '2015-09-24', '2015-09-23', '2015-09-22', '2015-09-21', '2015-09-20', '2015-09-19',
 '2015-09-18', '2015-09-17', '2015-09-16', '2015-09-15', '2015-09-14', '2015-09-13', '2015-09-12']


for d in l:
    now = datetime.datetime.now()
    if (10 < now.hour < 20) & (now .weekday() < 5):
        print('{}. Waiting for non-working time'.format(now))
        sys.stdout.flush()
        time.sleep(60 * 30)
    else:
        print('{}. Updating {}'.format(now,d))
        sys.stdout.flush()
        cursor.execute(query_pattern.replace('#visit_ymd', d))

print('done')