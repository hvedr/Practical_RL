
# coding: utf-8

import datetime,time
import os
import re

#constants
segment_nm = 'la_apppr_ccall_2'
clean_after_self = True
days_to_keep = 5 # Only relevant if clean_after_self = True

print('{0}: prod_lookalike.{1} update started.').format(datetime.datetime.now(),segment_nm)

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

#TODO 
urlfr_coeff_update_pattern = '''
insert overwrite table prod_lookalike.urlfr_coeff partition (segment_nm)
select urlfr, score,current_timestamp() as load_dttm, 'la_apppr_ccall_2' as segment_nm
     from prod_features_liveinternet.urlfr_tgt_cnt_cumulative2
     where target  = 'tinkoff_platinum_approved_application03@tinkoff_action'      
       and ymd = '2017-02-12'
       and (cnt_total > 300000 or cnt_positive > 10)

'''


def get_last_visits_date(cursor):
    cursor.execute('select max(ymd) from prod_odd.visit_feature')
    return cursor.fetchone()[0]

def get_last_model_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-02-12") from prod_lookalike.phone_x_segment where segment_nm = "#segment_nm"'.replace('#segment_nm', segment_nm))
    return cursor.fetchone()[0]

ymd_loaded     = get_last_visits_date(cursor) 
ymd_calculated = get_last_model_calc_date(cursor) 

cnt = 0
while ymd_calculated >= ymd_loaded:    
    cnt += 1
    if cnt > 70:
        print('{}. Failed to wait for visits to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd_loaded) + '*'*60)
        exit(1)
    print('Sleeping')
    time.sleep(1000)
    ymd_loaded = get_last_visits_date(cursor)

print('{} Got visits for {}. Calculating signals later than {}'.format(datetime.datetime.now(),ymd_loaded,ymd_calculated))
#ind = ymd_loaded.replace('-','')


queries = '''

insert overwrite table prod_lookalike.#segment_nm_raw partition (ymd)
select
    m.phone_num   
   ,max(t.score) as score  
   ,'#segment_nm' as segment_nm
   ,current_timestamp() as load_dttm
   ,v.ymd
from
    (select 
       uid_str as id,
       property_value as phone_num,
       load_src
     from
       prod_dds.md_uid_property 
     where
       property_cd = 'PHONE'
    ) m
    inner join prod_odd.visit_feature v on v.id = m.id and v.load_src = m.load_src
    inner join prod_lookalike.urlfr_coeff t on t.urlfr = v.url_fragment
where 
  v.ymd > '#ymd_calculated'
  and t.segment_nm = '#segment_nm'
group by m.phone_num, v.ymd
;




insert into prod_lookalike.phone_x_segment partition (segment_nm, ymd)
select
    a.phone_num
   ,a.score
   ,current_timestamp() as load_dttm
   ,a.segment_nm as segment_nm
   ,a.ymd as ymd
from
  prod_lookalike.#segment_nm_raw a
  left join prod_lookalike.threshold th on th.segment_nm = a.segment_nm
where
  a.score >= nvl(th.threshold, -999999)
  and a.ymd > '#ymd_calculated'
;
'''.replace('#ymd_calculated',ymd_calculated).replace('#segment_nm',segment_nm)

#print(queries)

for q in queries.split(';'):
    if re.search('[^ \n\t]',q):
        #print(q)
        cursor.execute(q)



os.popen('touch /home/bigdatasys/projects/la_ccall/#segment_nm_#ind_ready'
             .replace('#segment_nm',segment_nm)
             .replace('#ind',(datetime.datetime.today() - datetime.timedelta(days = 1)).strftime('%Y%m%d').replace('-',''))
        ).read()


if (clean_after_self):
    cursor.execute('''
       select distinct ymd
       from prod_lookalike.#segment_nm_raw 
       where segment_nm = '#segment_nm'
       order by ymd
       '''.replace('#segment_nm',segment_nm).replace('#days_to_keep', str(days_to_keep))
    )
    dates_to_delete = [e[0] for e in cursor.fetchall()[:-days_to_keep]]
    for ymd in dates_to_delete:
        cursor.execute("alter table prod_lookalike.#segment_nm_raw drop partition (ymd = '#ymd')".replace('#segment_nm',segment_nm).replace('#ymd',ymd))
    print('prod_lookalike.#segment_nm_raw cleaned for {} dates.'.format(','.join(dates_to_delete)).replace('#segment_nm',segment_nm))
    
print('{0}: prod_lookalike.{1} update successfully finished.').format(datetime.datetime.now(),segment_nm)
