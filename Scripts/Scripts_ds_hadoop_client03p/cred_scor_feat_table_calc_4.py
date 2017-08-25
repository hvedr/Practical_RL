# coding: utf-8

import datetime,time
import os, sys
import re

print('{0}: Calc cred scoring feat acc.').format(datetime.datetime.now())

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



class calc_cred_score():

    init_query = '''
    set hive.vectorized.execution.enabled=true;
    set mapreduce.map.memory.mb=4096;
    set mapreduce.map.child.java.opts=-Xmx4g;
    set mapreduce.task.io.sort.mb=1024;
    set mapreduce.reduce.child.java.opts=-Xmx4g;
    set mapreduce.reduce.memory.mb=7000;
    set mapreduce.reduce.shuffle.input.buffer.percent=0.5;
    set mapreduce.input.fileinputformat.split.minsize=536870912;
    set mapreduce.input.fileinputformat.split.maxsize=1073741824;
    set hive.optimize.ppd=true;
    set hive.merge.smallfiles.avgsize=536870912;
    set hive.merge.mapredfiles=true;
    set hive.merge.mapfiles=true;
    set hive.hadoop.supports.splittable.combineinputformat=true;
    set hive.exec.reducers.bytes.per.reducer=536870912;
    set hive.exec.parallel=true;
    set hive.exec.max.created.files=10000000;
    set hive.exec.compress.output=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=1000000;
    set hive.exec.max.dynamic.partitions.pernode=100000;
    set io.seqfile.compression.type=BLOCK;
    set mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
    set mapreduce.map.failures.maxpercent=5;

    set hive.tez.auto.reducer.parallelism=true;
    set hive.tez.min.partition.factor=0.25;
    set hive.tez.max.partition.factor=2.0;
    set tez.runtime.pipelined.sorter.lazy-allocate.memory=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set mapred.job.queue.name=bigdata_long;
    '''

    create_accum_query = '''
    CREATE TABLE IF NOT EXISTS `user_kposminin.id_feat_accum`(
          `id` string, 
          `load_src` string, 
          `first_id_ymd` string, 
          `last_id_ymd` string, 
          `ymd_cnt` int, 
          `urlfr_cnt` bigint, -- `cnt` bigint, 
          `visits_cnt` bigint, 
          `hits` bigint, 
          `emailru_sum` bigint, 
          `mobile_sum` double, 
          `vk_sum` double, 
          `social_sum` double, 
          `work_hours_hits_sum` double, 
          `avg_hour_sum_sq` bigint, 
          `avg_hour_sum` bigint, 
          `max_score1` double, 
          `max_score2` double, 
          `max_score3` double, 
          `max_score4` double, 
          `max_score5` double, 
          `max_score6` double, 
          `min_score1` double, 
          `min_score2` double, 
          `min_score3` double, 
          `min_score4` double, 
          `min_score5` double, 
          `min_score6` double, 
          `sum_score1` double, 
          `sum_score2` double, 
          `sum_score3` double, 
          `sum_score4` double, 
          `sum_score5` double, 
          `sum_score6` double, 
          `cnt_score1` int, 
          `cnt_score2` int, 
          `cnt_score3` int, 
          `cnt_score4` int, 
          `cnt_score5` int, 
          `cnt_score6` int, 
          `good_urlfr_sum_score1` double, 
          `good_urlfr_sum_score2` double, 
          `good_urlfr_sum_score3` double,
          `good_urlfr_sum_score4` double,
          `good_urlfr_sum_score5` double,
          `good_urlfr_sum_score6` double)
    partitioned by (
          `first_calc_ymd` string, 
          `last_calc_ymd` string      
          )
    ;

    '''

    feat_1d_query_pattern = '''

    -- #ymd. Id_feats 1 day calc
    with mymd as 
     (select
       target,
       max(ymd) as max_ymd
      from
       user_kposminin.urlfr_scores
      where 
        ymd < date_add('#ymd',-3)
      group by target
     )

    insert overwrite table user_kposminin.id_feat_accum partition (first_calc_ymd, last_calc_ymd)
    select 
     id,
     load_src, 
     ymd as first_id_ymd, 
     ymd as last_id_ymd,
     1 as ymd_cnt,
     count(distinct urlfr) as urlfr_cnt,
     count(urlfr) as visits_cnt,
     sum(cnt) as hits,
     sum(if(urlfr like 'e.mail.ru%',1,0)) as emailru_sum,
     sum(if(urlfr like 'm.%',1,0)) as mobile_sum,
     sum(if(urlfr rlike '^(m\\.)?vk.com', 1, 0)) as vk_sum,
     sum(if(urlfr rlike '^(m\\.)?vk.com' or urlfr rlike '^(m\\.)?(ok|odnoklassniki)\\.ru' or urlfr rlike '^(m\\.)?my.mail.ru',1,0)) as social_sum,
     sum(if(avg_hour >= 9 and avg_hour <= 20,cnt,0)) as work_hours_hits_sum,
     sum( avg_hour * avg_hour) as avg_hour_sum_sq,
     sum(avg_hour) as avg_hour_sum,
     max(score1) as max_score1,
     max(score2) as max_score2,
     max(score3) as max_score3,
     max(score4) as max_score4,
     max(score5) as max_score5,
     max(score6) as max_score6,
     min(score1) as min_score1,
     min(score2) as min_score2,
     min(score3) as min_score3,
     min(score4) as min_score4,
     min(score5) as min_score5,
     min(score6) as min_score6,
     sum(score1) as sum_score1,
     sum(score2) as sum_score2,
     sum(score3) as sum_score3,
     sum(score4) as sum_score4, 
     sum(score5) as sum_score5,
     sum(score6) as sum_score6, 
     count(score1) as cnt_score1,
     count(score2) as cnt_score2,
     count(score3) as cnt_score3,
     count(score4) as cnt_score4, 
     count(score5) as cnt_score5,
     count(score6) as cnt_score6, 
     count( if(score1 > -0.2, urlfr,Null)) as good_urlfr_sum_score1,
     count( if(score2 > -9, urlfr,Null)) as good_urlfr_sum_score2,
     count( if(score3 > -9, urlfr,Null)) as good_urlfr_sum_score3,
     count( if(score4 > -2, urlfr,Null)) as good_urlfr_sum_score4,
     count( if(score5 > -9, urlfr,Null)) as good_urlfr_sum_score5,
     count( if(score6 > -0.2, urlfr,Null)) as good_urlfr_sum_score6,
     '#ymd' as first_calc_ymd, 
     '#ymd' as last_calc_ymd

    from
     (select
        v.id,
        'LI.02' as load_src, -- !!!! bug
        v.ymd,
        v.urlfr,
        unix_timestamp(v.ymd, 'yyyy-MM-dd')/60/60 + v.avg_hour  as time_h,
        1 as time_std,
        v.cnt as cnt,
        v.avg_hour as avg_hour,
        t1.score as score1,
        t2.score as score2,
        t3.score as score3,
        t4.score as score4,
        t5.score as score5,
        log((t2.cnt_positive + 0.1)/(t3.cnt_positive - t2.cnt_positive + 0.1)) as score6
      from
        user_kposminin.ccall_visits v
        left join (
            select urlfr,score
              from mymd td
             inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'ccall_tinkoff_approve_from_fullapp'
        ) t1 on t1.urlfr = v.urlfr
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_approved_application03@tinkoff_action' 
        ) t2 on t2.urlfr = v.urlfr
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_complete_application03@tinkoff_action'
        ) t3 on t3.urlfr = v.urlfr
        left join (
            select urlfr,score
              from mymd td
             inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_LON_CCR_default'
        ) t4 on t4.urlfr = v.urlfr
        left join (
            select urlfr,score
              from mymd td
             inner join user_kposminin.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_approved_application03_1m'
        ) t5 on t5.urlfr = v.urlfr
      where 
        v.ymd = '#ymd' 
     ) a 
    group by
      id,load_src,ymd
    ;
    '''

    accumulator_merge_pattern = '''

    insert overwrite table user_kposminin.id_feat_accum partition (first_calc_ymd,last_calc_ymd)
    select
      nvl(a.id,b.id) as id,
      nvl(a.load_src,b.load_src) as load_src,
      least(a.first_id_ymd,b.first_id_ymd) as first_id_ymd,
      greatest(a.last_id_ymd,b.last_id_ymd) as last_id_ymd,
      nvl(a.ymd_cnt,0) + nvl(b.ymd_cnt,0) as ymd_cnt,
      nvl(a.urlfr_cnt,0) + nvl(b.urlfr_cnt,0) as urlfr_cnt,
      nvl(a.visits_cnt,0) + nvl(b.visits_cnt,0) as visits_cnt,
      nvl(a.hits,0) + nvl(b.hits,0) as hits,
      nvl(a.emailru_sum,0) + nvl(b.emailru_sum,0) as emailru_sum,
      nvl(a.mobile_sum,0) + nvl(b.mobile_sum,0) as mobile_sum,
      nvl(a.vk_sum,0) + nvl(b.vk_sum,0) as vk_sum,
      nvl(a.social_sum,0) + nvl(b.social_sum,0) as social_sum,
      nvl(a.work_hours_hits_sum,0) + nvl(b.work_hours_hits_sum,0) as work_hours_hits_sum,
      nvl(a.avg_hour_sum_sq,0) + nvl(b.avg_hour_sum_sq,0) as avg_hour_sum_sq,
      nvl(a.avg_hour_sum,0) + nvl(b.avg_hour_sum,0) as avg_hour_sum,
      greatest(a.max_score1,b.max_score1) as max_score1,
      greatest(a.max_score2,b.max_score2) as max_score2,
      greatest(a.max_score3,b.max_score3) as max_score3,
      greatest(a.max_score4,b.max_score4) as max_score4,
      greatest(a.max_score5,b.max_score5) as max_score5,
      greatest(a.max_score6,b.max_score6) as max_score6,
      least(a.min_score1, b.min_score1) as min_score1,
      least(a.min_score2, b.min_score2) as min_score2,
      least(a.min_score3, b.min_score3) as min_score3,
      least(a.min_score4, b.min_score4) as min_score4,
      least(a.min_score5, b.min_score5) as min_score5,
      least(a.min_score6, b.min_score6) as min_score6,
      nvl(a.sum_score1,0) + nvl(b.sum_score1,0) as sum_score1,
      nvl(a.sum_score2,0) + nvl(b.sum_score2,0) as sum_score2,
      nvl(a.sum_score3,0) + nvl(b.sum_score3,0) as sum_score3,
      nvl(a.sum_score4,0) + nvl(b.sum_score4,0) as sum_score4,
      nvl(a.sum_score5,0) + nvl(b.sum_score5,0) as sum_score5,
      nvl(a.sum_score6,0) + nvl(b.sum_score6,0) as sum_score6,
      nvl(a.cnt_score1,0) + nvl(b.cnt_score1,0) as cnt_score1,
      nvl(a.cnt_score2,0) + nvl(b.cnt_score2,0) as cnt_score2,
      nvl(a.cnt_score3,0) + nvl(b.cnt_score3,0) as cnt_score3,
      nvl(a.cnt_score4,0) + nvl(b.cnt_score4,0) as cnt_score4,
      nvl(a.cnt_score5,0) + nvl(b.cnt_score5,0) as cnt_score5,
      nvl(a.cnt_score6,0) + nvl(b.cnt_score6,0) as cnt_score6,
      nvl(a.good_urlfr_sum_score1,0) + nvl(b.good_urlfr_sum_score1,0) as good_urlfr_sum_score1,
      nvl(a.good_urlfr_sum_score2,0) + nvl(b.good_urlfr_sum_score2,0) as good_urlfr_sum_score2,
      nvl(a.good_urlfr_sum_score3,0) + nvl(b.good_urlfr_sum_score3,0) as good_urlfr_sum_score3,
      nvl(a.good_urlfr_sum_score4,0) + nvl(b.good_urlfr_sum_score4,0) as good_urlfr_sum_score4,
      nvl(a.good_urlfr_sum_score5,0) + nvl(b.good_urlfr_sum_score5,0) as good_urlfr_sum_score5,
      nvl(a.good_urlfr_sum_score6,0) + nvl(b.good_urlfr_sum_score6,0) as good_urlfr_sum_score6,
      '#new_first_calc_ymd' as first_calc_ymd,
      '#new_last_calc_ymd' as last_calc_ymd
    from 
      (select * from user_kposminin.id_feat_accum a where a.first_calc_ymd = '#a_first_calc_ymd' and a.last_calc_ymd = '#a_last_calc_ymd') a
      full join (select * from user_kposminin.id_feat_accum b where b.first_calc_ymd = '#b_first_calc_ymd' and b.last_calc_ymd = '#b_last_calc_ymd') b 
        on a.id = b.id and a.load_src = b.load_src
    ;


    '''

    clear_partition_query = '''
    alter table user_kposminin.id_feat_accum drop partition (first_calc_ymd = '#first_calc_ymd', last_calc_ymd = '#last_calc_ymd');
    '''

    create_feat_table_query = '''

    create table user_kposminin.id_cred_scor_test_20170429 as
    select 
         a.phone_mobile, 
         count(distinct m.id) as id_cnt,
         count(distinct acc.id) as acc_id_cnt,
         a.ymd as call_ymd,
         max(a.approve) as approve,
         max(a.utilization) as utilization, 
         max(a.in_work) as in_work, 
         a.utm_campaign, 
         max(a.considered) as considered,
         sum(acc.urlfr_cnt) as urlfr_cnt,
         sum(acc.visits_cnt) as visits_cnt, 
         sum(acc.hits) as hits,
         max(acc.max_score1) as max_score1,
         sum(acc.sum_score1) / sum(acc.cnt_score1) as avg_score1,
         min(acc.min_score1) as min_score1,
         max(acc.max_score2) as max_score2,
         sum(acc.sum_score2) / sum(acc.cnt_score2) as avg_score2,
         min(acc.min_score2) as min_score2,
         max(acc.max_score3) as max_score3,
         sum(acc.sum_score3) / sum(acc.cnt_score3) as avg_score3,
         min(acc.min_score3) as min_score3,
         max(acc.max_score4) as max_score4,
         sum(acc.sum_score4) / sum(acc.cnt_score4) as avg_score4,
         min(acc.min_score4) as min_score4,
         sum(acc.emailru_sum) as emailru_sum, 
         sum(acc.mobile_sum) / sum(acc.visits_cnt) as mobile_share,
         0 as vk_share, -- !!! Bug in train data
         sum(acc.social_sum) / sum(acc.visits_cnt) as social_share,
         sum(acc.work_hours_hits_sum) / sum(acc.hits) as work_hours_hits_share,
         sqrt(sum(acc.avg_hour_sum_sq)/(sum(acc.visits_cnt)-1) - power(sum(acc.avg_hour_sum)/(sum(acc.visits_cnt) - 1), 2)) as hour_std,
         sum(acc.good_urlfr_sum_score1) / sum(acc.cnt_score1) as good_urlfr_share_score1,  -- !!! Bug in calc method/ must be sum(cnt_score)
         sum(acc.good_urlfr_sum_score2) / sum(acc.cnt_score2) as good_urlfr_share_score2,  -- !!! Bug in calc method/ must be sum(cnt_score)
         sum(acc.good_urlfr_sum_score3) / sum(acc.cnt_score3) as good_urlfr_share_score3,  -- !!! Bug in calc method/ must be sum(cnt_score)
         max(trim(pc.provider)) as mob_provider,
         max(r.ind) as ind,
         max(r.pop_country_share) as pop_country_share, 
         max(r.pop_city_share) as pop_city_share, 
         max(r.population / r.area_sq_km) as density,
         max(r.area_sq_km) as area_sq_km,
         max(trim(r.federal_district)) as federal_district, 
         max(r.avg_salary_2015_rub) as avg_salary_2015_rub, 
         max(r.utc_time_zone_val) as utc_time_zone_val


      from
         user_kposminin.full_app_201704 a
         inner join 
          (select 
             uid_str as id,
             property_value as phone_num,
             load_src
           from
             prod_dds.md_uid_property 
           where
             property_cd = 'PHONE'
          ) m on substr(m.phone_num,3,20) = substr(a.phone_mobile,2,20)

         inner join user_kposminin.id_feat_accum acc on acc.id = m.id and acc.last_calc_ymd = date_add(a.ymd, -1)
         left join dds_dic.phone_codes pc on trim(pc.phone_code) = substr(a.phone_mobile,2,9)
         left join dds_dic.region_stat r on r.ind = pc.region_id
         where a.ymd = '2017-04-29'
           and acc.first_calc_ymd = '2017-03-01'
     group by
             a.phone_mobile,
             a.ymd,
             a.utm_campaign
    ;


    '''
    
    def __init__(self, hc = None, n_threads = 10):
        import datetime
        self.query = self.init_query + self.create_accum_query
        
    def calc_days(self, days, merge = True, clean = True):
        days = sorted(days)
        acc_first_calc_ymd, acc_last_calc_ymd = [days[0]] * 2
        for day in days:
            new_first_calc_ymd = min(day,acc_first_calc_ymd)
            new_last_calc_ymd  = max(day,acc_last_calc_ymd)
            
            self.query += self.feat_1d_query_pattern.replace('#ymd',day.strftime('%Y-%m-%d'))
            if merge & (days.index(day) > 0):
                self.query += (self.accumulator_merge_pattern 
                       .replace('#a_first_calc_ymd', acc_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#a_last_calc_ymd', acc_last_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#b_first_calc_ymd', day.strftime('%Y-%m-%d'))
                       .replace('#b_last_calc_ymd', day.strftime('%Y-%m-%d'))
                       .replace('#new_first_calc_ymd', new_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#new_last_calc_ymd',  new_last_calc_ymd.strftime('%Y-%m-%d'))
                               )
                if clean & (days.index(day) > 1):
                    self.query += (self.clear_partition_query 
                       .replace('#first_calc_ymd', acc_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd', acc_last_calc_ymd.strftime('%Y-%m-%d'))
                                  )
                if ((clean == 'full_clean') & (days.index(day) > 1)): 
                    self.query += (self.clear_partition_query 
                       .replace('#first_calc_ymd', day.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd', day.strftime('%Y-%m-%d'))
                                  )
            acc_first_calc_ymd = new_first_calc_ymd
            acc_last_calc_ymd = new_last_calc_ymd
    
    def calc_day_range(self, first_day = None, last_day = None, n_days = None, merge = True, clean = True):        
        assert (first_day is None) + (last_day is None) + (n_days is None) == 1, '''calc_day_range Error:
              exactly two of three params must be filled: first_day, last_day, n_days'''.replace('\n',' ')
        if first_day is None:
            day_range = [last_day + datetime.timedelta(days = i) for i in range(-n_days + 1,1)]
        elif last_day is None:
            day_range = [first_day + datetime.timedelta(days = i) for i in range(n_days)]
        else:
            first_day,last_day = min(first_day,last_day), max(first_day,last_day)
            day_range = [first_day + datetime.timedelta(days = i) for i in range((last_day - first_day).days + 1)]
        self.calc_days(day_range, merge, clean)
        
    def get_query(self):
        return self.query



def exec_queries(cursor,queries_str):
    '''
       Execute queries using cursor. Queries are in a string separated by ; symbol
    '''
    for q in queries_str.split(';'):
        if re.search('[^ \t\n]',q):
            cursor.execute(q)

def main():

    first_day = datetime.datetime.strptime(sys.argv[1],'%Y-%m-%d')
    last_day  = datetime.datetime.strptime(sys.argv[2],'%Y-%m-%d')



    c = calc_cred_score()
    c.calc_day_range(first_day = first_day, last_day = last_day, merge = True, clean = True)
    for q in c.get_query().split(';'):
        print('{}. Executing {}\n\n\n'.format(datetime.datetime.now(),q))
        exec_queries(cursor,q)

    print('End')

main()