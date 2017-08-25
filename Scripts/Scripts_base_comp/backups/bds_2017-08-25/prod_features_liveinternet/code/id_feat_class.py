
# coding: utf-8
import datetime, re



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
    '''

    create_accum_query = '''
    CREATE TABLE IF NOT EXISTS `prod_features_liveinternet.id_feat_accum`(
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
          `max_score7` double, 
          `max_score8` double, 
          `min_score1` double, 
          `min_score2` double, 
          `min_score3` double, 
          `min_score4` double, 
          `min_score5` double, 
          `min_score6` double, 
          `min_score7` double, 
          `min_score8` double, 
          `sum_score1` double, 
          `sum_score2` double, 
          `sum_score3` double, 
          `sum_score4` double, 
          `sum_score5` double, 
          `sum_score6` double, 
          `sum_score7` double, 
          `sum_score8` double, 
          `cnt_score1` int, 
          `cnt_score2` int, 
          `cnt_score3` int, 
          `cnt_score4` int, 
          `cnt_score5` int, 
          `cnt_score6` int, 
          `cnt_score7` int, 
          `cnt_score8` int, 
          `good_urlfr_sum_score1` double, 
          `good_urlfr_sum_score2` double, 
          `good_urlfr_sum_score3` double,
          `good_urlfr_sum_score4` double,
          `good_urlfr_sum_score5` double,
          `good_urlfr_sum_score6` double,
          `good_urlfr_sum_score7` double,
          `good_urlfr_sum_score8` double)
    partitioned by (
          `first_calc_ymd` string, 
          `last_calc_ymd` string      
          )
    stored as RCFile
    ;
 
    CREATE TABLE `prod_features_liveinternet.phone_x_feature`(
	  `phone_num` string, 
	  `id_cnt` bigint, 
	  `acc_id_cnt` bigint, 
	  `urlfr_cnt` bigint, 
	  `visits_cnt` bigint, 
	  `hits` bigint, 
	  `max_score1` double, 
	  `avg_score1` double, 
	  `min_score1` double, 
	  `max_score2` double, 
	  `avg_score2` double, 
	  `min_score2` double, 
	  `max_score3` double, 
	  `avg_score3` double, 
	  `min_score3` double, 
	  `max_score4` double, 
	  `avg_score4` double, 
	  `min_score4` double, 
	  `max_score5` double, 
	  `avg_score5` double, 
	  `min_score5` double, 
	  `max_score6` double, 
	  `avg_score6` double, 
	  `min_score6` double, 
	  `max_score7` double, 
	  `avg_score7` double, 
	  `min_score7` double, 
	  `max_score8` double, 
	  `avg_score8` double, 
	  `min_score8` double, 
	  `emailru_share` double, 
	  `mobile_share` double, 
	  `vk_share` double, 
	  `social_share` double, 
	  `work_hours_hits_share` double, 
	  `hour_std` double, 
	  `good_urlfr_share_score1` double, 
	  `good_urlfr_share_score2` double, 
	  `good_urlfr_share_score3` double, 
	  `good_urlfr_share_score4` double, 
	  `good_urlfr_share_score5` double, 
	  `good_urlfr_share_score6` double, 
	  `good_urlfr_share_score7` double, 
	  `good_urlfr_share_score8` double, 
	  `mob_provider` string, 
	  `ind` tinyint, 
	  `pop_country_share` float, 
	  `pop_city_share` float, 
	  `density` double, 
	  `area_sq_km` int, 
	  `federal_district` string, 
	  `avg_salary_2015_rub` int, 
	  `utc_time_zone_val` float)
    partitioned by (
	  `first_calc_ymd` string, 
	  `last_calc_ymd` string)
    stored as RCFile
    ;

    '''
 
    feat_1d_query_pattern = '''

    -- #ymd. Id_feats 1 day calc
    with mymd as 
     (select
       target,
       max(ymd) as max_ymd
      from
       prod_features_liveinternet.urlfr_scores
      where 
        ymd < date_add('#ymd',-3)
      group by target
     )

    insert overwrite table prod_features_liveinternet.id_feat_accum partition (first_calc_ymd, last_calc_ymd)
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
     max(score7) as max_score7,
     max(score8) as max_score8,
     min(score1) as min_score1,
     min(score2) as min_score2,
     min(score3) as min_score3,
     min(score4) as min_score4,
     min(score5) as min_score5,
     min(score6) as min_score6,
     min(score7) as min_score7,
     min(score8) as min_score8,
     sum(score1) as sum_score1,
     sum(score2) as sum_score2,
     sum(score3) as sum_score3,
     sum(score4) as sum_score4, 
     sum(score5) as sum_score5,
     sum(score6) as sum_score6, 
     sum(score7) as sum_score7,
     sum(score8) as sum_score8, 
     count(score1) as cnt_score1,
     count(score2) as cnt_score2,
     count(score3) as cnt_score3,
     count(score4) as cnt_score4, 
     count(score5) as cnt_score5,
     count(score6) as cnt_score6, 
     count(score7) as cnt_score7,
     count(score8) as cnt_score8, 
     count( if(score1 > -0.2, urlfr,Null)) as good_urlfr_sum_score1,
     count( if(score2 > -9, urlfr,Null)) as good_urlfr_sum_score2,
     count( if(score3 > -9, urlfr,Null)) as good_urlfr_sum_score3,
     count( if(score4 > -2, urlfr,Null)) as good_urlfr_sum_score4,
     count( if(score5 > -9, urlfr,Null)) as good_urlfr_sum_score5,
     count( if(score6 > -0.2, urlfr,Null)) as good_urlfr_sum_score6,
     count( if(score7 > -9, urlfr,Null)) as good_urlfr_sum_score7,
     count( if(score8 > -0.2, urlfr,Null)) as good_urlfr_sum_score8,
     '#ymd' as first_calc_ymd, 
     '#ymd' as last_calc_ymd
 
    from
     (select
        v.id,
        v.load_src as load_src,
        v.ymd,
        v.url_fragment as urlfr,
        unix_timestamp(v.ymd, 'yyyy-MM-dd')/60/60 + v.average_visit_hour  as time_h,
        1 as time_std,
        v.visit_count as cnt,
        v.average_visit_hour as avg_hour,
        t1.score as score1,
        t2.score as score2,
        t3.score as score3,
        t4.score as score4,
        t5.score as score5,
        log((t2.cnt_positive + 0.1)/(t3.cnt_positive - t2.cnt_positive + 0.1)) as score6,
        t7.score as score7,
        log((t5.cnt_positive + 0.1)/(t7.cnt_positive - t5.cnt_positive + 0.1)) as score8
      from
        prod_odd.visit_feature v
        left join (
            select urlfr,score
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'ccall_tinkoff_approve_from_fullapp'
        ) t1 on t1.urlfr = v.url_fragment
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_approved_application03@tinkoff_action' 
        ) t2 on t2.urlfr = v.url_fragment
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_complete_application03@tinkoff_action'
        ) t3 on t3.urlfr = v.url_fragment
        left join (
            select urlfr,score
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_LON_CCR_default'
        ) t4 on t4.urlfr = v.url_fragment
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_approved_application03_1m'
        ) t5 on t5.urlfr = v.url_fragment
        left join (
            select urlfr,score,positive as cnt_positive
              from mymd td
             inner join prod_features_liveinternet.urlfr_scores t on t.target = td.target and t.ymd = td.max_ymd
             where td.target = 'tinkoff_platinum_complete_application03_1m'
        ) t7 on t7.urlfr = v.url_fragment
      where 
        v.ymd = '#ymd' 
     ) a 
    group by
      id,load_src,ymd
    ;
    '''

    accumulator_merge_pattern = '''

    insert overwrite table prod_features_liveinternet.id_feat_accum partition (first_calc_ymd,last_calc_ymd)
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
      greatest(a.max_score7,b.max_score7) as max_score7,
      greatest(a.max_score8,b.max_score8) as max_score8,
      least(a.min_score1, b.min_score1) as min_score1,
      least(a.min_score2, b.min_score2) as min_score2,
      least(a.min_score3, b.min_score3) as min_score3,
      least(a.min_score4, b.min_score4) as min_score4,
      least(a.min_score5, b.min_score5) as min_score5,
      least(a.min_score6, b.min_score6) as min_score6,
      least(a.min_score7, b.min_score7) as min_score7,
      least(a.min_score8, b.min_score8) as min_score8,
      nvl(a.sum_score1,0) + nvl(b.sum_score1,0) as sum_score1,
      nvl(a.sum_score2,0) + nvl(b.sum_score2,0) as sum_score2,
      nvl(a.sum_score3,0) + nvl(b.sum_score3,0) as sum_score3,
      nvl(a.sum_score4,0) + nvl(b.sum_score4,0) as sum_score4,
      nvl(a.sum_score5,0) + nvl(b.sum_score5,0) as sum_score5,
      nvl(a.sum_score6,0) + nvl(b.sum_score6,0) as sum_score6,
      nvl(a.sum_score7,0) + nvl(b.sum_score7,0) as sum_score7,
      nvl(a.sum_score8,0) + nvl(b.sum_score8,0) as sum_score8,
      nvl(a.cnt_score1,0) + nvl(b.cnt_score1,0) as cnt_score1,
      nvl(a.cnt_score2,0) + nvl(b.cnt_score2,0) as cnt_score2,
      nvl(a.cnt_score3,0) + nvl(b.cnt_score3,0) as cnt_score3,
      nvl(a.cnt_score4,0) + nvl(b.cnt_score4,0) as cnt_score4,
      nvl(a.cnt_score5,0) + nvl(b.cnt_score5,0) as cnt_score5,
      nvl(a.cnt_score6,0) + nvl(b.cnt_score6,0) as cnt_score6,
      nvl(a.cnt_score7,0) + nvl(b.cnt_score7,0) as cnt_score7,
      nvl(a.cnt_score8,0) + nvl(b.cnt_score8,0) as cnt_score8,
      nvl(a.good_urlfr_sum_score1,0) + nvl(b.good_urlfr_sum_score1,0) as good_urlfr_sum_score1,
      nvl(a.good_urlfr_sum_score2,0) + nvl(b.good_urlfr_sum_score2,0) as good_urlfr_sum_score2,
      nvl(a.good_urlfr_sum_score3,0) + nvl(b.good_urlfr_sum_score3,0) as good_urlfr_sum_score3,
      nvl(a.good_urlfr_sum_score4,0) + nvl(b.good_urlfr_sum_score4,0) as good_urlfr_sum_score4,
      nvl(a.good_urlfr_sum_score5,0) + nvl(b.good_urlfr_sum_score5,0) as good_urlfr_sum_score5,
      nvl(a.good_urlfr_sum_score6,0) + nvl(b.good_urlfr_sum_score6,0) as good_urlfr_sum_score6,
      nvl(a.good_urlfr_sum_score7,0) + nvl(b.good_urlfr_sum_score7,0) as good_urlfr_sum_score7,
      nvl(a.good_urlfr_sum_score8,0) + nvl(b.good_urlfr_sum_score8,0) as good_urlfr_sum_score8,
      '#new_first_calc_ymd' as first_calc_ymd,
      '#new_last_calc_ymd' as last_calc_ymd
    from 
      (select * from prod_features_liveinternet.id_feat_accum a where a.first_calc_ymd = '#a_first_calc_ymd' and a.last_calc_ymd = '#a_last_calc_ymd') a
      full join (select * from prod_features_liveinternet.id_feat_accum b where b.first_calc_ymd = '#b_first_calc_ymd' and b.last_calc_ymd = '#b_last_calc_ymd') b 
        on a.id = b.id and a.load_src = b.load_src
    ;


    '''

    clear_partition_query = '''
    alter table prod_features_liveinternet.id_feat_accum drop partition (first_calc_ymd = '#first_calc_ymd', last_calc_ymd = '#last_calc_ymd');
    '''

    create_feat_table_query = '''



    insert overwrite table prod_features_liveinternet.phone_x_feature partition (first_calc_ymd,last_calc_ymd)
    select
         m.phone_num,
         count(distinct m.id) as id_cnt,
         count(distinct acc.id) as acc_id_cnt,
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
         max(acc.max_score5) as max_score5,
         sum(acc.sum_score5) / sum(acc.cnt_score5) as avg_score5,
         min(acc.min_score5) as min_score5,
         max(acc.max_score6) as max_score6,
         sum(acc.sum_score6) / sum(acc.cnt_score6) as avg_score6,
         min(acc.min_score6) as min_score6,
         max(acc.max_score7) as max_score7,
         sum(acc.sum_score7) / sum(acc.cnt_score7) as avg_score7,
         min(acc.min_score7) as min_score7,
         max(acc.max_score8) as max_score8,
         sum(acc.sum_score8) / sum(acc.cnt_score8) as avg_score8,
         min(acc.min_score8) as min_score8,
         sum(acc.emailru_sum) / sum(acc.visits_cnt) as emailru_share,
         sum(acc.mobile_sum) / sum(acc.visits_cnt) as mobile_share,
         sum(acc.vk_sum) / sum(acc.visits_cnt) as vk_share,
         sum(acc.social_sum) / sum(acc.visits_cnt) as social_share,
         sum(acc.work_hours_hits_sum) / sum(acc.hits) as work_hours_hits_share,
         sqrt(sum(acc.avg_hour_sum_sq)/(sum(acc.visits_cnt)-1) - power(sum(acc.avg_hour_sum)/(sum(acc.visits_cnt) - 1), 2)) as hour_std,
         sum(acc.good_urlfr_sum_score1) / sum(acc.cnt_score1) as good_urlfr_share_score1,
         sum(acc.good_urlfr_sum_score2) / sum(acc.cnt_score2) as good_urlfr_share_score2,
         sum(acc.good_urlfr_sum_score3) / sum(acc.cnt_score3) as good_urlfr_share_score3,
         sum(acc.good_urlfr_sum_score4) / sum(acc.cnt_score4) as good_urlfr_share_score4,
         sum(acc.good_urlfr_sum_score5) / sum(acc.cnt_score5) as good_urlfr_share_score5,
         sum(acc.good_urlfr_sum_score6) / sum(acc.cnt_score6) as good_urlfr_share_score6,
         sum(acc.good_urlfr_sum_score7) / sum(acc.cnt_score7) as good_urlfr_share_score7,
         sum(acc.good_urlfr_sum_score8) / sum(acc.cnt_score8) as good_urlfr_share_score8,
         max(trim(pc.provider)) as mob_provider,
         max(r.ind) as ind,
         max(r.pop_country_share) as pop_country_share,
         max(r.pop_city_share) as pop_city_share,
         max(r.population / r.area_sq_km) as density,
         max(r.area_sq_km) as area_sq_km,
         max(trim(r.federal_district)) as federal_district,
         max(r.avg_salary_2015_rub) as avg_salary_2015_rub,
         max(r.utc_time_zone_val) as utc_time_zone_val,
         '#first_calc_ymd' as first_calc_ymd,
         '#last_calc_ymd' as last_calc_ymd
    
      from
         prod_features_liveinternet.id_feat_accum acc
         inner join (
                   select
                     uid_str as id,
                     property_value as phone_num,
                     load_src
                   from
                     prod_dds.md_uid_property
                   where
                     property_cd = 'PHONE'
                  ) m on m.id = acc.id
         left join dds_dic.phone_codes pc on trim(pc.phone_code) = substr(m.phone_num,3,9)
         left join dds_dic.region_stat r on r.ind = pc.region_id
      where
         acc.first_calc_ymd = '#first_calc_ymd'
         and acc.last_calc_ymd  = '#last_calc_ymd'
     group by
             m.phone_num             
    ;

    '''
    
    clean_full_feat_partition_query = '''
    alter table prod_features_liveinternet.phone_x_feature drop partition (first_calc_ymd = '#first_calc_ymd', last_calc_ymd = '#last_calc_ymd');
    '''
    
    
    def __init__(self, hc = None, n_threads = 10, init_query = False):
        import datetime
        if(init_query):
            self.query = self.init_query + self.create_accum_query
        else:
            self.query = ''            
        self.log = ''
        
    def calc_days(self, days, merge = True, clean = True):
        ''' Calc 1d feat and accum it for days (datetime.datetime list)'''
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
    
    def merge_days(self, day_tuples, clean = False):
        ''' day_tuples is a list of (first_day,last_day) datetime instances to merge'''
        day_tuples = sorted(day_tuples)
        acc_first_calc_ymd, acc_last_calc_ymd = day_tuples[0]
        first = True
        for (first_day,last_day) in day_tuples[1:]:
            new_first_calc_ymd = min(first_day,acc_first_calc_ymd)
            new_last_calc_ymd  = max(last_day,acc_last_calc_ymd)
            self.query += (self.accumulator_merge_pattern 
                       .replace('#a_first_calc_ymd', acc_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#a_last_calc_ymd',   acc_last_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#b_first_calc_ymd', first_day.strftime('%Y-%m-%d'))
                       .replace('#b_last_calc_ymd',   last_day.strftime('%Y-%m-%d'))
                       .replace('#new_first_calc_ymd', new_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#new_last_calc_ymd',   new_last_calc_ymd.strftime('%Y-%m-%d'))
                               )
            if (not clean is False) & ((not first) or (clean == 'full_clean')):
                self.query += (self.clear_partition_query 
                       .replace('#first_calc_ymd', acc_first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd',   acc_last_calc_ymd.strftime('%Y-%m-%d'))
                                  )
            if (clean == 'full_clean'): 
                self.query += (self.clear_partition_query 
                       .replace('#first_calc_ymd', first_day.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd',   last_day.strftime('%Y-%m-%d'))
                                  )
            acc_first_calc_ymd = new_first_calc_ymd
            acc_last_calc_ymd = new_last_calc_ymd
            first = False
            
    def calc_full_feat(self,first_calc_ymd, last_calc_ymd):
        ''' Calculate full phone features table.  '''
        self.query += (self.create_feat_table_query 
                           .replace('#first_calc_ymd', first_calc_ymd.strftime('%Y-%m-%d'))
                           .replace('#last_calc_ymd',   last_calc_ymd.strftime('%Y-%m-%d'))
		      )
        
    def drop_partitions(self,day_tuples):
        ''' drop partitions from day_tuples '''
        for fd,ld in day_tuples:
            self.query += (self.clear_partition_query 
                       .replace('#first_calc_ymd', fd.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd', ld.strftime('%Y-%m-%d'))
            )

    def drop_partition(self,first_calc_ymd,last_calc_ymd):
        ''' drop partition '''
        self.drop_partitions([(first_calc_ymd,last_calc_ymd)])

    def clean_full_feat_partitions(self,day_tuples):
        ''' drop partitions from day_tuples '''
        for fd,ld in day_tuples:
            self.query += (self.clean_full_feat_partition_query 
                       .replace('#first_calc_ymd', fd.strftime('%Y-%m-%d'))
                       .replace('#last_calc_ymd', ld.strftime('%Y-%m-%d'))
            )

    def execute_query(self, hc = None, cursor = None):
        '''Execute query using hc HiveContext'''
        assert (hc is None) + (cursor  is None) == 1, \
          'Exactly one of two params: HiveContext hc or hive.Connection.cursor cursor has to be provided'
        for q in self.query.split(';'):
            if re.search('[^ \t\n]',q):
                self.log += 'Executing {}\n'.format(q)
                if hc:
                    hc.sql(q)
                else:
                    cursor.execute(q)
    
    def get_log(self):
        return log
