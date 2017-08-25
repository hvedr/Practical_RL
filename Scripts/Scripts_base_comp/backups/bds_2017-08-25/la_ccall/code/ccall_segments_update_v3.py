#Updated 2017-06-22 to serve several tables in prod_ccall. One segment at most can be loaded into one destination table.

# coding: utf-8

import datetime,time
import os
import pandas as pd
import sys

print('{0}: ccall segments update started.'.format(datetime.datetime.now()))
sys.stdout.flush()

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

seg_ready_files_path = '/home/bigdatasys/projects/la_ccall'

cursor.execute('select date_sub(to_date(current_timestamp()),1)')
yesterday = cursor.fetchone()[0]
ind = yesterday.replace('-','')

cursor.execute('select destination, segment_nm from prod_lookalike.segments_to_load')
df_segs = pd.DataFrame([[e[0],e[1],'',0,0] for e in cursor.fetchall()], columns = ['destination', 'segment_nm', 'ymd_loaded', 'raw_ready', 'updated'])

print '{}.df_segs initial'.format(datetime.datetime.now()) 
print df_segs

for tab_name in df_segs['destination'].unique():
    cursor.execute('''select nvl(max(ymd),'2017-01-01') from #tab_name'''.replace('#tab_name',tab_name))
    df_segs.loc[df_segs['destination'] == tab_name,'ymd_loaded'] = cursor.fetchone()[0]

print '{}.df_segs after adding destination'.format(datetime.datetime.now()) 
print df_segs
sys.stdout.flush()


print('{}. Waiting for data to load segments into destination tables for dates later than:\n{}.'.format(
               datetime.datetime.now(), 
               '\n'.join(df_segs[['segment_nm','destination','ymd_loaded']].apply(lambda r: ','.join(r), axis = 1))
               ))

sys.stdout.flush()

cnt = 0
file_list = os.popen('ls '+ seg_ready_files_path).read().split('\n')
df_segs['raw_ready'] = df_segs['segment_nm'].map(lambda s: '{}_{}_ready'.format(s,ind) in file_list)

print '{}.df_segs after adding raw_ready'.format(datetime.datetime.now()) 
print df_segs
sys.stdout.flush()





#This query combines all phone numbers from phones_x_segments according to score and segment share and puts it into phones_to_call
query_pattern = '''
with csymd as 
  (
    select nvl(max(ymd),'2017-01-01') as mymd
    from prod_lookalike.phone_x_segment
    where segment_nm = 'cred_score_1'
  )
insert overwrite table #tab_name partition(ymd)
select
   phone_num
  ,row_number() over (partition by ymd order by rnk) as rnk
  ,segment_nm
  ,seg_score
  ,cred_score
  ,current_timestamp() as load_dttm
  ,ymd  
from
  (
  select
     phone_num
    ,max(rnk) as rnk
    ,max(
      named_struct(
        'rnk', rnk,
        'segment_nm',segment_nm
        )
     ).segment_nm as segment_nm
    ,max(
      named_struct(
        'rnk', rnk,
        'score',score
        )
     ).score as seg_score
    ,max(
      named_struct(
        'rnk', rnk,
        'cred_score',cred_score
        )
     ).cred_score as cred_score
    ,ymd
  from
    (
     select
       a.ymd
      ,a.phone_num
      ,b.coef * (rank() over (partition by a.ymd, a.segment_nm order by a.score desc)) as rnk
      ,a.segment_nm
      ,a.score
      ,c.score as cred_score
     from
      prod_lookalike.phone_x_segment a
      inner join
        (
          select
            segment_nm
          , exp(sum(log(share)) over (partition by destination))/share as coef
          , cred_threshold
          from prod_lookalike.segments_to_load
          where destination = '#tab_name'
          and segment_nm in (#segs)
        ) b on b.segment_nm = a.segment_nm
      left join csymd csymd
      left join prod_lookalike.phone_x_segment c on c.phone_num = a.phone_num and csymd.mymd = c.ymd
     where a.ymd > '#ymd_loaded'
       and nvl(c.score,1) >= b.cred_threshold
    ) d
  group by phone_num,ymd
  ) f

'''

while df_segs['updated'].mean() != 1:
    while df_segs[df_segs['updated'] == 0]['raw_ready'].sum() == 0:
        cnt += 1
        if cnt > 70:
            #print('{}. Failed to wait for segments to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd_loaded) + '*' * 60)
            print('{}. Failed to wait for smth. Terminating.\n'.format(datetime.datetime.now()) + '*' * 60)
            sys.stdout.flush()
            exit(1)
        print '{} Falling asleep for 1000 sec'.format(datetime.datetime.now())   
        sys.stdout.flush()             
        time.sleep(1000)

        file_list = os.popen('ls '+ seg_ready_files_path).read().split('\n')
        df_segs['raw_ready'] = df_segs['segment_nm'].map(lambda s: '{}_{}_ready'.format(s,ind) in file_list)
        print '{}.df_segs after updating raw_ready'.format(datetime.datetime.now()) 
        print df_segs
        sys.stdout.flush()

    idx = df_segs[(df_segs['updated'] == 0) & (df_segs['raw_ready'] == 1)].index
    print('{}. Processing:\n{}.'.format(
               datetime.datetime.now(), 
               '\n'.join(df_segs.ix[idx,['segment_nm','destination','ymd_loaded']].apply(lambda r: ','.join(r), axis = 1))
               ))
    sys.stdout.flush()
    for tab_name, dest in df_segs.ix[idx].groupby('destination'):
        segs = "'" + "','".join(df_segs[df_segs['destination'] == tab_name]['segment_nm'].values) + "'"
        #segs = "'" + "','".join(dest['segment_nm'].values) + "'"
        ymd_loaded = dest['ymd_loaded'].iloc[0]
        q = query_pattern.replace('#segs',segs).replace('#ymd_loaded',ymd_loaded).replace('#tab_name',tab_name)
        print(q)
        sys.stdout.flush()
        cursor.execute(query_pattern.replace('#segs',segs).replace('#ymd_loaded',ymd_loaded).replace('#tab_name',tab_name))
    df_segs.ix[idx,'updated'] = 1

    print '{}.df_segs after updating updated to value 1'.format(datetime.datetime.now()) 
    print df_segs
    sys.stdout.flush()

    for s in df_segs.ix[idx,'segment_nm']:
        print('rm command to execute:  ' + 'rm -f {}/{}_{}_ready'.format(seg_ready_files_path,s,ind))
        sys.stdout.flush()
        os.popen('rm -f {}/{}_{}_ready'.format(seg_ready_files_path,s,ind)).read()
    
    print('{}. Following ccall segment(s) successfully updated: {}.'.format(datetime.datetime.now(), segs))
    sys.stdout.flush()

for f in df_segs['segment_nm']:
    os.popen('rm -f {}/{}_{}_ready'.format(seg_ready_files_path,s,ind)).read()

print('{}: All ccall segments update successfully finished. Program shuts down.\n'.format(datetime.datetime.now()))

