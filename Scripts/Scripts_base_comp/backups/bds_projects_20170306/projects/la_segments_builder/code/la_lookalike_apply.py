# -*- coding: utf-8 -*-
# При запуске код строит look-alike сегменты на данных за вчерашний день и записывает их в prod_lookalike.user_x_segment.
# Url_parts со скорами и названиями сегментов берутся из таблицы prod_lookalike.lookalike_coeff.
# Для добавления нового сегмента нужно добавить его url_parts с коэффициентами в prod_lookalike.lookalike_coeff и он начнет строиться автоматически.
# По окончанию работы создается файл /home/bigdatasys/projects/la_segments_builder/new_segments_ready.
# Запуск этого файла установлен в CRONTAB bigdatasys

from os import path
from time import sleep

def visits_ready():
    res = path.exists('/home/bigdatasys/projects/prod_features_liveinternet/visits_ready')
    return res

while not visits_ready():
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

queries = []

queries.append('drop table if exists prod_lookalike.la_compl_ind_0'.replace('ind', ind))
queries.append("""
create table prod_lookalike.la_compl_ind_0 as
select
 a.id
 ,max(b.score) score
 ,b.segment_nm
 ,max("ymd_dt") as ymd
from
 prod_features_liveinternet.visits a
 inner join prod_lookalike.urlfr_coeff b on a.urlfr = b.urlfr 
 left join prod_lookalike.threshold t on t.segment_nm = b.segment_nm
where 
 a.ymd in ("ymd_dt")
 and b.score >= nvl(t.threshold,-999999)
 and b.segment_nm in ('2016-08-19_2016-09-18_tinkoff_platinum_complete_application@tinkoff_action')
group by
  a.id
 ,b.segment_nm
""".replace('ind',ind).replace('ymd_dt', ymd)
)

queries.append('drop table if exists prod_lookalike.la_compl_ind_1'.replace('ind', ind))
queries.append("""
create table prod_lookalike.la_compl_ind_1 as
select
 b.h_uid_rk
 ,max(a.score) score
 ,a.segment_nm
 ,a.ymd
from
 prod_lookalike.la_compl_ind_0 a
 inner join prod_dds.h_uid b on a.id = b.uid_str
where
 b.load_src = 'LI.02'
group by
 b.h_uid_rk
 ,a.segment_nm
 ,a.ymd
""".replace('ind',ind).replace('ymd_dt', ymd)
)

queries.append("""
insert overwrite
 table prod_lookalike.user_x_segment partition(segment_nm, ymd)
select
 h_uid_rk
 ,score
 ,current_timestamp() as load_dttm
 ,segment_nm
 ,ymd
from
 prod_lookalike.la_compl_ind_1""".replace('ind',ind)
)
i=0
for query in queries:
    print query
    print('{1}. Executing query {0}.'.format(i,datetime.datetime.now()))
    i += 1
    cursor.execute(query)
for query in queries:    
    if 'drop table' in query:
        cursor.execute(query)

cursor.close()

from subprocess import call

call('touch /home/bigdatasys/projects/la_segments_builder/new_segments_ready', shell = True)
call('rm /home/bigdatasys/projects/prod_features_liveinternet/visits_ready', shell = True)

print('\nla_lookalike_apply for {0} SUCCESS. {1}.\n'.format(ymd, datetime.datetime.now()) + '*' * 30 + '\n')