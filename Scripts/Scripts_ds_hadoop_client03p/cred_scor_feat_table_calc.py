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



queries = '''

set mapred.job.queue.name=bigdata_long;


-- id_feat 2017-02-16
create table user_kposminin.id_feat_1d_20170216 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-16',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-16' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-17
create table user_kposminin.id_feat_1d_20170217 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-17',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-17' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-18
create table user_kposminin.id_feat_1d_20170218 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-18',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-18' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-19
create table user_kposminin.id_feat_1d_20170219 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-19',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-19' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-20
create table user_kposminin.id_feat_1d_20170220 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-20',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-20' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-21
create table user_kposminin.id_feat_1d_20170221 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-21',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-21' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-22
create table user_kposminin.id_feat_1d_20170222 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-22',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-22' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-23
create table user_kposminin.id_feat_1d_20170223 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-23',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-23' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-24
create table user_kposminin.id_feat_1d_20170224 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-24',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-24' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-25
create table user_kposminin.id_feat_1d_20170225 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-25',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-25' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-26
create table user_kposminin.id_feat_1d_20170226 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-26',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-26' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-27
create table user_kposminin.id_feat_1d_20170227 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-27',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-27' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-02-28
create table user_kposminin.id_feat_1d_20170228 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-02-28',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-02-28' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-01
create table user_kposminin.id_feat_1d_20170301 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-01',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-01' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-02
create table user_kposminin.id_feat_1d_20170302 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-02',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-02' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-03
create table user_kposminin.id_feat_1d_20170303 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-03',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-03' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-04
create table user_kposminin.id_feat_1d_20170304 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-04',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-04' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-05
create table user_kposminin.id_feat_1d_20170305 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-05',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-05' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-06
create table user_kposminin.id_feat_1d_20170306 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-06',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-06' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-07
create table user_kposminin.id_feat_1d_20170307 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-07',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-07' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-08
create table user_kposminin.id_feat_1d_20170308 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-08',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-08' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-09
create table user_kposminin.id_feat_1d_20170309 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-09',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-09' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-10
create table user_kposminin.id_feat_1d_20170310 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-10',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-10' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-11
create table user_kposminin.id_feat_1d_20170311 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-11',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-11' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-12
create table user_kposminin.id_feat_1d_20170312 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-12',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-12' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-13
create table user_kposminin.id_feat_1d_20170313 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-13',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-13' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-14
create table user_kposminin.id_feat_1d_20170314 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-14',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-14' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-15
create table user_kposminin.id_feat_1d_20170315 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-15',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-15' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-16
create table user_kposminin.id_feat_1d_20170316 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-16',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-16' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-17
create table user_kposminin.id_feat_1d_20170317 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-17',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-17' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-18
create table user_kposminin.id_feat_1d_20170318 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-18',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-18' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-19
create table user_kposminin.id_feat_1d_20170319 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-19',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-19' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-20
create table user_kposminin.id_feat_1d_20170320 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-20',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-20' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-21
create table user_kposminin.id_feat_1d_20170321 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-21',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-21' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-22
create table user_kposminin.id_feat_1d_20170322 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-22',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-22' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-23
create table user_kposminin.id_feat_1d_20170323 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-23',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-23' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-24
create table user_kposminin.id_feat_1d_20170324 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-24',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-24' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-25
create table user_kposminin.id_feat_1d_20170325 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-25',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-25' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-26
create table user_kposminin.id_feat_1d_20170326 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-26',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-26' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-27
create table user_kposminin.id_feat_1d_20170327 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-27',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-27' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-28
create table user_kposminin.id_feat_1d_20170328 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-28',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-28' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-29
create table user_kposminin.id_feat_1d_20170329 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-29',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-29' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-30
create table user_kposminin.id_feat_1d_20170330 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-30',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-30' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-03-31
create table user_kposminin.id_feat_1d_20170331 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-03-31',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-03-31' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-01
create table user_kposminin.id_feat_1d_20170401 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-01',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-01' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-02
create table user_kposminin.id_feat_1d_20170402 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-02',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-02' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-03
create table user_kposminin.id_feat_1d_20170403 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-03',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-03' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-04
create table user_kposminin.id_feat_1d_20170404 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-04',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-04' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-05
create table user_kposminin.id_feat_1d_20170405 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-05',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-05' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-06
create table user_kposminin.id_feat_1d_20170406 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-06',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-06' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-07
create table user_kposminin.id_feat_1d_20170407 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-07',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-07' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-08
create table user_kposminin.id_feat_1d_20170408 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-08',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-08' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-09
create table user_kposminin.id_feat_1d_20170409 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-09',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-09' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-10
create table user_kposminin.id_feat_1d_20170410 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-10',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-10' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-11
create table user_kposminin.id_feat_1d_20170411 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-11',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-11' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-12
create table user_kposminin.id_feat_1d_20170412 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-12',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-12' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-13
create table user_kposminin.id_feat_1d_20170413 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-13',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-13' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-14
create table user_kposminin.id_feat_1d_20170414 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-14',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-14' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-15
create table user_kposminin.id_feat_1d_20170415 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-15',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-15' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-16
create table user_kposminin.id_feat_1d_20170416 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-16',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-16' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-17
create table user_kposminin.id_feat_1d_20170417 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-17',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-17' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-18
create table user_kposminin.id_feat_1d_20170418 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-18',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-18' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-19
create table user_kposminin.id_feat_1d_20170419 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-19',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-19' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-20
create table user_kposminin.id_feat_1d_20170420 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-20',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-20' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-21
create table user_kposminin.id_feat_1d_20170421 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-21',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-21' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-22
create table user_kposminin.id_feat_1d_20170422 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-22',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-22' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-23
create table user_kposminin.id_feat_1d_20170423 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-23',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-23' 
 ) a 
group by
  id,load_src,ymd
;


-- id_feat 2017-04-24
create table user_kposminin.id_feat_1d_20170424 as
with mymd as 
 (select
   target,
   max(ymd) as max_ymd
  from
   user_kposminin.urlfr_tgt_cnt
  where 
    ymd < date_add('2017-04-24',-3)
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
 sum(if(urlfr rlike '^(m\.)?vk.com%', 1, 0))/sum(1) as vk_share,
 sum(if(urlfr like 'vk.com%' or urlfr rlike '^(m\.)?ok\.ru' or urlfr like 'm.odnoklassniki.ru%' or urlfr rlike '^(m\.)?my.mail.ru',1,0))/sum(1) as social_share,
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
    v.ymd = '2017-04-24' 
 ) a 
group by
  id,load_src,ymd
;




create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170123 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170124 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170125 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170126 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170127 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170128 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170129 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170130 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170131 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170201 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170202 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170203 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170204 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170205 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170206 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170207 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170208 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170209 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170210 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170211 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170212 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170213 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170214 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170215 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170216 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170217 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170218 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170219 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170220 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170221 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170222 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170223 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170224 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170225 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170226 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170227 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170228 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170301 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170302 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170303 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170304 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170305 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170306 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170307 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170308 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170309 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170310 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170311 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170312 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170313 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170314 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170315 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170316 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170317 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170318 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170319 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170320 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170321 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170322 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170323 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170324 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170325 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170326 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170327 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170328 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170329 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170330 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170331 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170401 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170402 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170403 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170404 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170405 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170406 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170407 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170408 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170409 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170410 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170411 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170412 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170413 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170414 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170415 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170416 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170417 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170418 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170419 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170420 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170421 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170422 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170423 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


create table user_kposminin.id_feat_accum_upd as
select
 nvl(a.id,b.id) as id,
 nvl(a.load_src,b.load_src) as load_src,
least(a.first_ymd,b.ymd) as first_ymd,
greatest(a.last_ymd,b.ymd) as last_ymd,
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
(nvl(a.good_urlfr_share_score3 * a.visits_cnt,0) + nvl(b.good_urlfr_share_score3 * b.visits_cnt,0)) / (nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0)) as good_urlfr_share_score3


from 
  user_kposminin.id_feat_accum a
  full join user_kposminin.id_feat_1d_20170424 b on a.id = b.id and a.load_src = b.load_src
;

drop table user_kposminin.id_feat_accum;
alter table user_kposminin.id_feat_accum_upd rename to user_kposminin.id_feat_accum;


'''

for q in queries.split(';'):
    print('{}. Executing:\n{}'.format(datetime.datetime.now(),q))
    sys.stdout.flush()
    cursor.execute(q)
print('End')