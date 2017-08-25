# -*- coding: utf-8 -*-
import datetime
print('\n' + '*' * 40 + '\n{0}: Update tinkoff actions started.'.format(datetime.datetime.now()))
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
          }
clean_after_self = True
from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()
#extract last update date
cursor.execute('select max(ymd) from prod_features_liveinternet.tinkoff_actions where CAST(ymd AS Date) IS NOT NULL')
try:
    last_upd_ymd = cursor.fetchone()[0]
except IndexError:
    print('Error connecting to prod_features_liveinternet')
    exit(1)
if(datetime.date.today() > datetime.datetime.strptime(last_upd_ymd, "%Y-%m-%d").date()):
    queries = '''
    drop table if exists prod_features_liveinternet.cc_wuid_#ind;
    create table prod_features_liveinternet.cc_wuid_#ind as
    select distinct 
        dt_created,
        ymd, 
        wuid, 
        (case is_processed when 3 then 1 else 0 end) as completed_flag,
        0 as revisited
    from prod_dds.portal_application 
    where ymd > '#last_upd_ymd'
    and ymd < '#yesterday'
    and CAST(ymd AS Date) IS NOT NULL
    and wuid is not null 
    and product_name = 'cc_platinum' 
    and lower(lp) not like '%agent%'
    and is_processed = 3
    and linked_id is Null
    ;
    
    insert into prod_features_liveinternet.cc_wuid_#ind
    select
        min(dt_created) as dt_created,
        min(ymd) as ymd,
        min(
           named_struct(
            'dt_created', dt_created,
            'wuid', wuid
           )
        ).wuid as wuid,
        1 as completed_flag,
        1 as revisited
    from
        (
        select
            s.id,
            f.dt_created,
            f.ymd,
            f.wuid
        from (select id, dt_created, ymd,wuid,is_processed,linked_id
              from prod_dds.portal_application
              where    is_processed = 3            
                   and lower(lp) not like '%agent%'
                   and product_name = 'cc_platinum' 
                   and not linked_id is Null
                   and ymd > '#last_upd_ymd' 
                   and ymd < '#yesterday'
              ) s
        inner join (select id, dt_created, ymd,wuid,is_processed,linked_id
              from prod_dds.portal_application
              where    is_processed = 21 
                   and lower(lp) not like '%agent%'
                   and product_name = 'cc_platinum'            
                   and dt_created >= '#prev_two_months'
              ) f on s.linked_id = f.id
        ) wd
    where CAST(ymd AS Date) IS NOT NULL
    group by id
    ;

    drop table if exists prod_features_liveinternet.cc_wuid_li_#ind;
    create table prod_features_liveinternet.cc_wuid_li_#ind as
    select
         a.ymd
        ,a.dt_created
        ,a.wuid
        ,a.completed_flag
        ,a.revisited
        ,b.dmp_id
        ,c.source_id as li_id
    from
     prod_features_liveinternet.cc_wuid_#ind a
     inner join (select distinct source_id, dmp_id from prod_emart.datamind_matching_table where source_type = 'tcs') b on a.wuid = b.source_id
     inner join (select distinct source_id, dmp_id from prod_emart.datamind_matching_table where source_type = 'liveinternet') c on b.dmp_id = c.dmp_id
    ;
    
    drop table if exists prod_features_liveinternet.cc_wuid_li_unique_#ind;
    create table prod_features_liveinternet.cc_wuid_li_unique_#ind as
    select 
      a.ymd
     ,a.li_id
     ,max(a.dt_created) as dt_created
     ,min(a.revisited) as revisited
     ,max(a.completed_flag) as completed_flag 
    from 
     prod_features_liveinternet.cc_wuid_li_#ind a
     inner join 
    (
    select
     wuid
     ,count(distinct li_id) li_cnt
    from
     prod_features_liveinternet.cc_wuid_li_#ind
    group by
     wuid
    having
     li_cnt = 1
    ) t on a.wuid = t.wuid
    where not li_id is NULL
    group by a.ymd, a.li_id
    ;
    
    insert into prod_features_liveinternet.tinkoff_actions partition (action_type)
    select 
        li_id as id,
        dt_created as time,
        ymd,
        'tinkoff_platinum_complete_application' as action_type
    from prod_features_liveinternet.cc_wuid_li_unique_#ind
    ;
    '''.replace('#ind',str(datetime.date.today()-datetime.timedelta(days=1)).replace('-','')).replace('#last_upd_ymd',last_upd_ymd) \
       .replace('#prev_two_months',str(datetime.datetime.strptime(last_upd_ymd, "%Y-%m-%d").date() - datetime.timedelta(weeks=9)))
       .replace('#yesterday',str(datetime.date.today()-datetime.timedelta(days=1)))

    for q in queries.split(';')[:-1]: cursor.execute(q)
    if(clean_after_self): 
        for q in queries.split(';')[:-1]: 
            if('drop table ' in q): cursor.execute(q)
    print('{0}: Tinkoff actions successfully updated.\n'.format(datetime.datetime.now()) + '*' * 40 + '\n\n')
