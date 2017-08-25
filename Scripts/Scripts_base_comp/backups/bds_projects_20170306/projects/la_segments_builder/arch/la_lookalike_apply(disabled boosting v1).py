# -*- coding: utf-8 -*-
# При запуске код строит look-alike сегменты на данных за вчерашний день и записывает их в prod_lookalike.user_x_segment.
# Url_parts со скорами и названиями сегментов берутся из таблицы prod_lookalike.lookalike_coeff.
# Для добавления нового сегмента нужно добавить его url_parts с коэффициентами в prod_lookalike.lookalike_coeff и он начнет строиться автоматически.
# По окончанию работы создается файл /home/bigdatasys/projects/la_segments_builder/new_segments_ready.
# Запуск этого файла установлен в CRONTAB bigdatasys на 00:30 ежедневно с логированием результатов в /home/bigdatasys/projects/la_segments_builder/log/log.log

from os import path
from time import sleep

def visits_ready():
    res = path.exists('/home/bigdatasys/projects/prod_features_liveinternet/visits_ready')
    return res

while visits_ready() == False:
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
          }


from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()

import datetime
from datetime import date, timedelta

ymd = str(date.today() - timedelta(1))
ind = ymd.replace('-', '')
print('*' * 30 + '\nla_lookalike_apply for {0} started at {1}.\n'.format(ymd, datetime.datetime.now()))

cursor.execute('drop table if exists prod_lookalike.la_ind_0'.replace('ind', ind))
cursor.execute("""
create table prod_lookalike.la_ind_0 
 row format
  delimited fields terminated by ';' 
 stored as INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
 OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' as
select
 c.h_uid_rk as id
 ,max(b.score) as max_score
 ,min(b.score) as min_score
 ,avg(b.score) as avg_score
 ,percentile_approx(b.score, 0.9) as q90_score
 ,percentile_approx(b.score, 0.75) as q75_score
 ,percentile_approx(b.score, 0.5) as q50_score
 ,percentile_approx(b.score,0.25) as q25_score
 ,count(*) as visited_urlfrs
 ,count(case when b.score > -5.7 then 1 end) as visited_good_urlfrs
 ,sum(a.cnt) as hits
 ,min(a.avg_hour) as min_avg_hour
 ,max(a.avg_hour) as max_avg_hour
 ,sum(if(a.urlfr like 'e.mail.ru%',1,0)) as emailru
 ,sum(if(a.urlfr like 'm.%',1,0))/sum(1) as mobile_share
 ,sum(if(a.urlfr rlike 'vk\\.com', 1, 0))/sum(1) as vk_share
 ,sum(if(a.urlfr rlike '^(m\\.)?ok\\.ru' or a.urlfr rlike 'm\\.odnoklassniki\\.ru',1,0))/sum(1) as ok_share
 ,max(
             named_struct( 
             'score',b.score, 
             'avg_hour',a.avg_hour  
             )           
          ).avg_hour as max_urlfr_avg_hour
  ,max(
             named_struct(
             'score', b.score,
             'hits', a.cnt
             )           
          ).hits as max_scored_urlfr_cnt
 ,max("ymd_dt") as ymd
from
 prod_features_liveinternet.visits a
 inner join prod_lookalike.lookalike_coeff b on a.urlfr = b.urlfr 
 inner join prod_dds.h_uid c on c.load_src = 'LI.02' and a.id = c.uid_str
 -- left join prod_lookalike.threshold t on t.segment_nm = b.segment_nm
where
 a.ymd in ("ymd_dt")
 and b.segment_nm = "2016-08-19_2016-09-18_tinkoff_platinum_complete_application@tinkoff_action"
 -- and a.score >= nvl(t.threshold,-999999)
group by
 c.h_uid_rk
order by
  max_score desc
limit
 10000000
""".replace('ind',ind).replace('ymd_dt', ymd)
)

cursor.execute('select count(*) from (select distinct id from prod_features_liveinternet.visits where ymd in ("ymd_dt")) t'.replace('ymd_dt', ymd))
cutoff = cursor.fetchone()[0]/1000

from sklearn.externals import joblib
from subprocess import call

adaboost_cl = joblib.load('/home/bigdatasys/projects/la_segments_builder/ccr_boosting_1d_intraday_labels.pkl')
local_data_file = '/home/bigdatasys/projects/la_segments_builder/data/la_ind_0.txt'.replace('ind',ind)
call(('hadoop fs -text /prod_lookalike/la_ind_0/* >' + local_data_file).replace('ind',ind)
     , shell = True)

import pandas as pd
data = pd.read_csv(local_data_file, sep=";", header = None)
data.columns = ['id','max_score','min_score','avg_score','q90_score','q75_score','q50_score','q25_score','visited_urlfrs'
                ,'visited_good_urlfrs', 'hits','min_avg_hour','max_avg_hour','emailru','mobile_share','vk_share','ok_share',
                'max_urlfr_avg_hour','max_scored_urlfr_cnt', 'ymd']

call('rm ' + local_data_file, shell = True)

data_selected = pd.DataFrame(sorted([line for line in zip(data['id'], adaboost_cl.predict_proba(data.iloc[:,1:19])[:,1], data['ymd'])], key = lambda x: -x[1])[:cutoff])     
data_selected.columns = ['h_uid_rk','score','ymd']


try:
    call('rm /home/bigdatasys/projects/la_segments_builder/data/selected.txt', shell = True)
except:
    pass
data_selected.to_csv('/home/bigdatasys/projects/la_segments_builder/data/selected.txt', index = False, header = False)
     
try:
    call('hadoop fs -rm -r /prod_lookalike/boosting/*', shell = True)    
except:
    pass
call('hadoop fs -put /home/bigdatasys/projects/la_segments_builder/data/selected.txt /prod_lookalike/boosting/selected.txt', shell = True)
     
cursor.execute('drop table if exists prod_lookalike.la_ind_1'.replace('ind', ind))
cursor.execute(
'''
create external table prod_lookalike.la_ind_1 (
h_uid_rk string
,score bigint
,ymd string)
ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
		WITH SERDEPROPERTIES ( 
	  'separatorChar'=',')
	STORED AS INPUTFORMAT 
	  'org.apache.hadoop.mapred.TextInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'hdfs://nameservice1/prod_lookalike/boosting'
'''.replace('ind', ind)
)
cursor.execute("""
insert overwrite
 table prod_lookalike.user_x_segment partition(segment_nm, ymd)
select
 h_uid_rk
 ,score
 ,current_timestamp() as load_dttm
 ,'boosting_v1' as segment_nm
 ,ymd
from
 prod_lookalike.la_ind_1""".replace('ind',ind)
)

cursor.execute('drop table if exists prod_lookalike.la_ind_0'.replace('ind', ind))
cursor.execute('drop table if exists prod_lookalike.la_ind_1'.replace('ind', ind))


cursor.close()


call('touch /home/bigdatasys/projects/la_segments_builder/new_segments_ready', shell = True)

print('\nla_lookalike_apply for {0} SUCCESS. {1}.\n'.format(ymd, datetime.datetime.now()) + '*' * 30 + '\n')

call('rm /home/bigdatasys/projects/prod_features_liveinternet/visits_ready', shell = True)