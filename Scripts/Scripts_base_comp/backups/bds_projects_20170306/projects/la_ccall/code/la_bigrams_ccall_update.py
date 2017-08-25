
# coding: utf-8

import datetime,time
import os
import re

#Constants
segment_nm = 'la_bigram_ccall'
clean_after_self = False
data_dir = '/home/bigdatasys/external_hdfs/ccall_bigram_data'


print('{}: {} segment update started.'.format(datetime.datetime.now(),segment_nm))

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


def get_last_visits_r_date(cursor):
    cursor.execute('select max(ymd) from prod_features_liveinternet.visits_r')
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

print('{} Got visits_r for {}. Calculating signals later than {}'.format(datetime.datetime.now(),ymd_loaded,ymd_calculated))
ind = ymd_loaded.replace('-','')

#add jar /opt/cloudera/parcels/hive-extensions/md5app_2.11-1.0.jar;
#CREATE FUNCTION md5 as 'onemd5.Md5';

sig_base_query = '''

drop table if exists prod_features_liveinternet.#segment_nm_raw_#ind;

create table prod_lookalike.#segment_nm_raw_#ind as
  select
      v.ymd
     ,b.contact_str as phone
     ,count(distinct v.id) as id_cnt
     ,concat_ws(' ',
       sort_array(
          split(
             concat_ws(' ',
                collect_set(
                    concat_ws(' ',
                        concat(lpad(conv(substr(md5(concat(v.utm,email_visit,ref_host,urlfr,'hsdk')),1,5),16,10),7,'0'),':1'),
                        concat(lpad(conv(substr(md5(concat(v.utm,email_visit,ref_host,urlfr,'ic2x')),1,5),16,10),7,'0'),':1'),
                        concat(lpad(conv(substr(md5(concat(v.utm,email_visit,ref_host,urlfr,'7pz0')),1,5),16,10),7,'0'),':1')
                    )
                )
             )
          ,' ')
       )
     ) as features_str
    from
      prod_features_liveinternet.visits_r v
        inner join (select uid_str,h_uid_rk from prod_dds.h_uid where load_src = 'LI.02') d on d.uid_str = v.id
        inner join (select h_contact_rk, h_uid_rk from prod_dds.l_uid_contact where contact_type_cd = 'PHONE') c on d.h_uid_rk = c.h_uid_rk
        inner join (select h_contact_rk, contact_str from prod_dds.h_contact where contact_type_cd = 'PHONE') b on b.h_contact_rk = c.h_contact_rk  
    where 
    split(urlfr,'#')[0] <> ref_host
    and v.ymd > '#ymd_calculated'
    group by b.contact_str,v.ymd



;
'''.replace('#ymd_calculated',ymd_calculated).replace('#ind',ind).replace('#segment_nm',segment_nm)

select_query = '''
select
  concat(
    ymd,
    '|',
    phone,
    ' ',
    features_str
  ) as out_str
from prod_lookalike.#segment_nm_raw_#ind

'''.replace('#ymd_calculated',ymd_calculated).replace('#ind',ind).replace('#segment_nm',segment_nm)

for q in sig_base_query.split(';'):
    if re.search('[^ \n\t]',q):
        #print(q)
        cursor.execute(q)

#print('\n'*2+select_query)

os.popen('hive -e "#select_query" > #data_dir/#segment_nm_#ind.txt'.replace('#ind',ind).replace('#select_query',select_query).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()
print('{}. Samplefile with {} rows downloaded. Starting prediction.'.format(datetime.datetime.now(),os.popen('wc -l ../data/#segment_nm_#ind.txt'.replace('#ind',ind).replace('#segment_nm',segment_nm)).read()))

os.popen('''perl -ne '@a = split(" ",$_); print join(" ",@a[1..$#a]) . "\n"'  #data_dir/#segment_nm_#ind.txt >  #data_dir/data/#segment_nm_features_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()
os.popen('''perl -ne '@a = split(" ",$_); print @a[0] . "\n"'  #data_dir/#segment_nm_#ind.txt >  #data_dir/#segment_nm_phones_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()
os.popen('''/opt/share/LightGBM-master/lightgbm task=predict input_model= ../bigram_model/bigr_model_trunc.model data=#data_dir/#segment_nm_features_#ind.txt output_result=../data/#segment_nm_predict_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()
os.popen('''paste  -d"|" #data_dir/#segment_nm_phones_#ind.txt #data_dir/#segment_nm_predict_#ind.txt > #data_dir/#segment_nm_phones_predict_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()
print('{}. Prediction done. Uploading to hive.'.format(datetime.datetime.now()))
os.popen('''hadoop fs -put #data_dir/#segment_nm_phones_predict_#ind.txt /user/bigdatasys/data/#segment_nm_phones_predict_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir')).read()

queries = '''

drop table if exists prod_lookalike.#segment_nm_#ind;

CREATE EXTERNAL TABLE prod_lookalike.#segment_nm_#ind (  
  ymd string,
  phone string,
  score float
)
row format delimited fields terminated by '|' stored as textfile;
;

LOAD DATA INPATH '/user/bigdatasys/data/#segment_nm_phones_predict_#ind.txt' OVERWRITE INTO TABLE prod_lookalike.#segment_nm_#ind;

insert into prod_lookalike.phone_x_segment partition (segment_nm, ymd)
select
    a.phone as phone_num
   ,a.score
   ,current_timestamp() as load_dttm
   ,'#segment_nm' as segment_nm
   ,a.ymd as ymd
from
  prod_lookalike.#segment_nm_#ind a
  left join prod_lookalike.threshold th on th.segment_nm = '#segment_nm'
where
  a.score >= nvl(th.threshold, -999999)
;

'''.replace('#ind',ind).replace('#segment_nm',segment_nm)

for q in queries.split(';'):
    if re.search('[^ \n\t]',q):
        print(q)
        cursor.execute(q)


print('{}. {} update and uploade into prod_lookalike.phone_x_segmentfor dates later than {} done.'.format(datetime.datetime.now(),segment_nm, ymd_calculated))



#TODO Clean after self
if (clean_after_self):
    for q in (sig_base_query + queries).split(';'):
        if 'drop table'in q:
            cursor.execute(q)
    os.popen('''rm -f #data_dir/la_bigram_#ind.txt #data_dir/#segment_nm_features_#ind.txt #data_dir/#segment_nm_phones_#ind.txt #data_dir/#segment_nm_predict_#ind.txt #data_dir/#segment_nm_phones_predict_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm).replace('#data_dir','data_dir'))
    os.popen('''hadoop fs -rm /user/bigdatasys/data/#segment_nm_phones_predict_#ind.txt'''.replace('#ind',ind).replace('#segment_nm',segment_nm))


os.popen('touch /home/bigdatasys/projects/la_ccall/#segment_nm_ready'.replace('#segment_nm',segment_nm))
print('{0}: prod_lookalike.stand_la_ccall update successfully finished.'.format(datetime.datetime.now()))

