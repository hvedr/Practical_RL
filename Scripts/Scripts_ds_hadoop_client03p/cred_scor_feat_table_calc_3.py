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

create_accum_pattern = '''

drop table if exists user_kposminin.id_feat_accum1;
CREATE TABLE `user_kposminin.id_feat_accum1`(
	  `id` string, 
	  `load_src` string, 
	  `first_id_ymd` string, 
	  `last_id_ymd` string, 
	  `ymd_cnt` int, 
	  `cnt` bigint, 
	  `visits_cnt` bigint, 
	  `hits` bigint, 
	  `emailru` bigint, 
	  `mobile_share` double, 
	  `vk_share` double, 
	  `social_share` double, 
	  `work_hours_hits_share` double, 
	  `avg_hour_sum_sq` bigint, 
	  `avg_hour_sum` bigint, 
	  `max_score1` double, 
	  `max_score2` double, 
	  `max_score3` double, 
	  `max_score4` double, 
	  `min_score1` double, 
	  `min_score2` double, 
	  `min_score3` double, 
	  `min_score4` double, 
	  `sum_score1` double, 
	  `sum_score2` double, 
	  `sum_score3` double, 
	  `sum_score4` double, 
	  `good_urlfr_share_score1` double, 
	  `good_urlfr_share_score2` double, 
	  `good_urlfr_share_score3` double,
	  `first_calc_ymd` string, 
	  `last_calc_ymd` string   
      )
;

'''

create_accum_store_pattern = '''
CREATE TABLE `user_kposminin.id_feat_accum_store`(
	  `id` string, 
	  `load_src` string, 
	  `first_id_ymd` string, 
	  `last_id_ymd` string, 
	  `ymd_cnt` int, 
	  `cnt` bigint, 
	  `visits_cnt` bigint, 
	  `hits` bigint, 
	  `emailru` bigint, 
	  `mobile_share` double, 
	  `vk_share` double, 
	  `social_share` double, 
	  `work_hours_hits_share` double, 
	  `avg_hour_sum_sq` bigint, 
	  `avg_hour_sum` bigint, 
	  `max_score1` double, 
	  `max_score2` double, 
	  `max_score3` double, 
	  `max_score4` double, 
	  `min_score1` double, 
	  `min_score2` double, 
	  `min_score3` double, 
	  `min_score4` double, 
	  `sum_score1` double, 
	  `sum_score2` double, 
	  `sum_score3` double, 
	  `sum_score4` double, 
	  `good_urlfr_share_score1` double, 
	  `good_urlfr_share_score2` double, 
	  `good_urlfr_share_score3` double)
partitioned by (
	  `first_calc_ymd` string, 
	  `last_calc_ymd` string      
      )
;

'''

feat_1d_query_pattern = '''

-- Факторы по кукам. #ymd
create table user_kposminin.id_feat_1d_#ind as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('#ymd',-3)
  group by target
 )

select 
 id,
 load_src, 
 count(distinct urlfr) as cnt,
 count(*) as visits_cnt,
 sum(cnt) as hits,
 sum(if(urlfr like 'e.mail.ru%',1,0)) as emailru,
 sum(if(urlfr like 'm.%',1,0))/sum(1) as mobile_share,
 sum(if(urlfr rlike '^(m\\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\\.)?ok\\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\\.)?my.mail.ru',1,0))/sum(1) as social_share,
 sum(if(avg_hour >= 9 and avg_hour <= 20,cnt,0))/sum(cnt) as work_hours_hits_share,
 sum( avg_hour * avg_hour) as avg_hour_sum_sq,
 sum(avg_hour) as avg_hour_sum,
 max(score1) as max_score1,
 max(score2) as max_score2,
 max(score3) as max_score3,
 max(score4) as max_score4,
 min(score1) as min_score1,
 min(score2) as min_score2,
 min(score3) as min_score3,
 min(score4) as min_score4,
 sum(score1) as sum_score1,
 sum(score2) as sum_score2,
 sum(score3) as sum_score3,
 sum(score4) as sum_score4, 
 count( if(score1 > 1, urlfr,Null))/sum(1) as good_urlfr_share_score1,
 count( if(score2 > -7, urlfr,Null))/sum(1) as good_urlfr_share_score2,
 count( if(score3 > -7, urlfr,Null))/sum(1) as good_urlfr_share_score3,
 ymd
 
from
 (select
    v.id,
    v.load_src,
    v.ymd,
    v.url_fragment as urlfr,
    unix_timestamp(v.ymd, 'yyyy-MM-dd')/60/60 + v.average_visit_hour  as time_h,
    1 as time_std,
    v.visit_count as cnt,
    v.average_visit_hour as avg_hour,
    t1.score as score1,
    t2.score as score2,
    t3.score as score3,
    t4.score as score4
  from
    prod_odd.visit_feature v
    left semi join (select 
       uid_str as id,
       property_value as phone_num,
       load_src
     from
       prod_dds.md_uid_property 
     where
       property_cd = 'PHONE'
    ) m on m.id = v.id and m.load_src = v.load_src  
    left join (
        select urlfr,score
          from mymd td
         inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
         where td.target = 'ccall_tinkoff_approve_from_fullapp'
    ) t1 on t1.urlfr = v.url_fragment
    left join (
        select urlfr,score
          from mymd td
         inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
         where td.target = 'tinkoff_platinum_approved_application03@tinkoff_action' 
    ) t2 on t2.urlfr = v.url_fragment
    left join (
        select urlfr,score
          from mymd td
         inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
         where td.target = 'tinkoff_platinum_complete_application03@tinkoff_action'
    ) t3 on t3.urlfr = v.url_fragment
    left join (
        select urlfr,score
          from mymd td
         inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
         where td.target = 'tinkoff_LON_CCR_default'
    ) t4 on t4.urlfr = v.url_fragment
  where 
    v.ymd = '#ymd' 
 ) a 
group by
  id,load_src,ymd
;
'''

accumulator_update_pattern = '''

create table user_kposminin.id_feat_accum_upd1 as
select
  nvl(a.id,b.id) as id,
  nvl(a.load_src,b.load_src) as load_src,
  least(a.first_id_ymd,b.ymd) as first_id_ymd,
  greatest(a.last_id_ymd,b.ymd) as last_id_ymd,
  nvl(a.ymd_cnt,0) + if(b.ymd is Null,0,1) as ymd_cnt,
  nvl(a.cnt,0) + nvl(b.cnt,0) as cnt,
  nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0) as visits_cnt,
  nvl(a.hits,0) + nvl(b.hits,0) as hits,
  nvl(a.emailru,0) + nvl(b.emailru,0) as emailru,
  (nvl(a.mobile_share * a.visits_cnt,0) + nvl(b.mobile_share * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as mobile_share,
  (nvl(a.vk_share * a.visits_cnt,0) + nvl(b.vk_share * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as vk_share,
  (nvl(a.social_share * a.visits_cnt,0) + nvl(b.social_share * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as social_share,
  (nvl(a.work_hours_hits_share * a.visits_cnt,0) + nvl(b.work_hours_hits_share * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as work_hours_hits_share,
  nvl(a.avg_hour_sum_sq,0) + nvl(b.avg_hour_sum_sq,0) as avg_hour_sum_sq,
  nvl(a.avg_hour_sum,0) + nvl(b.avg_hour_sum,0) as avg_hour_sum,
  greatest(a.max_score1,b.max_score1) as max_score1,
  greatest(a.max_score2,b.max_score2) as max_score2,
  greatest(a.max_score3,b.max_score3) as max_score3,
  greatest(a.max_score4,b.max_score4) as max_score4,
  least(a.max_score1,b.max_score1) as min_score1,
  least(a.max_score2,b.max_score2) as min_score2,
  least(a.max_score3,b.max_score3) as min_score3,
  least(a.max_score4,b.max_score4) as min_score4,
  nvl(a.sum_score1,0) + nvl(b.sum_score1,0) as sum_score1,
  nvl(a.sum_score2,0) + nvl(b.sum_score2,0) as sum_score2,
  nvl(a.sum_score3,0) + nvl(b.sum_score3,0) as sum_score3,
  nvl(a.sum_score4,0) + nvl(b.sum_score4,0) as sum_score4,
  (nvl(a.good_urlfr_share_score1 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score1 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score1,
  (nvl(a.good_urlfr_share_score2 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score2 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score2,
  (nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3,
  '#first_ymd' as first_calc_ymd,
  '#ymd' as last_calc_ymd
from 
  user_kposminin.id_feat_accum1 a
  full join user_kposminin.id_feat_1d_#ind b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum1;
alter table user_kposminin.id_feat_accum_upd1 rename to user_kposminin.id_feat_accum1;

'''

write_acc_query = '''
insert overwrite table user_kposminin.id_feat_accum_store partition (first_calc_ymd,last_calc_ymd)
select * from user_kposminin.id_feat_accum1;

'''


def exec_queries(cursor,queries_str):
    '''
       Execute queries using cursor. Queries are in a string separated by ; symbol
    '''
    for q in queries_str.split(';'):
        if re.search('[^ \t\n]',q):
            print('{}. Executing\n{}'.format(datetime.datetime.now(),q))
            sys.stdout.flush()
            cursor.execute(q)

def main():
    interval = 15
    acc_upd_query = ''
    start_dates = reduce(lambda x,y:x+y,[[datetime.datetime(2017, i, 1),datetime.datetime(2017, i, 16)]  for i in range(1, 6)])
    for first_ymd,next_ymd in zip(start_dates[:-1],start_dates[1:]):
        day = first_ymd
        acc_upd_query += create_accum_pattern
        while day < next_ymd:            
            acc_upd_query += (accumulator_update_pattern
                            .replace('#ymd',day.strftime('%Y-%m-%d'))
                            .replace('#ind',day.strftime('%Y%m%d'))
                            .replace('#first_ymd',first_ymd.strftime('%Y-%m-%d'))
                             )
            day = day + datetime.timedelta(days = 1)
        acc_upd_query += (write_acc_query
                                .replace('#ymd',day.strftime('%Y-%m-%d'))
                                .replace('#ind',day.strftime('%Y%m%d'))
                                .replace('#first_ymd',first_ymd.strftime('%Y-%m-%d'))
                                 )        
    exec_queries(cursor,acc_upd_query)
    #print(acc_upd_query)
    print('End')

main()