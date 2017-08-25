
set hive.vectorized.execution.enabled=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.child.java.opts=-Xmx4g;
set mapreduce.task.io.sort.mb=1024;
set mapreduce.reduce.child.java.opts=-Xmx4g;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.shuffle.input.buffer.percent=0.5;
set mapreduce.input.fileinputformat.split.minsize=268435456;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set hive.optimize.ppd=true;
set hive.merge.smallfiles.avgsize=268435456;
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.exec.parallel=true;
set hive.exec.max.created.files=100000000;
set hive.exec.compress.output=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000000;
set hive.exec.max.dynamic.partitions.pernode=1000000;
set io.seqfile.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set mapreduce.map.failures.maxpercent=5;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.min.partition.factor=0.25;
set hive.tez.max.partition.factor=2.0;
set tez.runtime.pipelined.sorter.lazy-allocate.memory=true;
set mapreduce.map.output.compress=true;
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

set CALC_LAST_DATE;

CREATE
 TABLE if not exists prod_features_liveinternet.url_fragment_target_cnt 
     (`url_fragment` string, 
	  `cnt_total` bigint, 
	  `cnt_positive` map<string,int>, 
	  `score` map<string,double>, 
	  `load_dttm` timestamp
     ) PARTITIONED BY 
     (ymd string)
STORED
    AS RCFile
    ;

with t as (

select url_fragment
     , count(distinct positive_tpca) as cnt_positive_tpca
     , count(distinct positive_tpaa) as cnt_positive_tpaa
     , count(distinct positive_aaca) as cnt_positive_aaca
     , count(distinct positive_aaaa) as cnt_positive_aaaa
     , count(distinct id) as cnt_total
     , "${hiveconf:CALC_LAST_DATE}" as ymd
  from(select vf.id
            , vf.url_fragment
            , case when ua.ymd between vf.ymd and date_add(vf.ymd, 3) and ua.action_type = 'tinkoff_platinum_complete_application' then ua.id end
           as positive_tpca
            , case when ua.ymd between vf.ymd and date_add(vf.ymd, 3) and ua.action_type = 'tinkoff_platinum_approved_application' then ua.id end
           as positive_tpaa
            , case when ua.ymd between vf.ymd and date_add(vf.ymd, 3) and ua.action_type = 'all_airlines_complete_application' then ua.id end
           as positive_aaca
            , case when ua.ymd between vf.ymd and date_add(vf.ymd, 3) and ua.action_type = 'all_airlines_approved_application' then ua.id end
           as positive_aaaa
         from prod_odd.visit_feature vf
         left 
         join (select distinct 
                      id
                    , action_type  
                    , ymd 
                 from prod_odd.user_action
                where ymd between "${hiveconf:CALC_LAST_DATE}" and date_add("${hiveconf:CALC_LAST_DATE}",3)
                  and action_type in ('tinkoff_platinum_complete_application', 'tinkoff_platinum_approved_application', 'all_airlines_complete_application', 'all_airlines_approved_application')
               ) ua on ua.id = vf.id
        where vf.ymd = "${hiveconf:CALC_LAST_DATE}"
          and trim(vf.url_fragment) != '#'
          and vf.load_src = 'LI.02'
        ) k
 group
    by url_fragment
 having (cnt_total > 500 or cnt_positive_tpca + cnt_positive_tpaa + cnt_positive_aaca + cnt_positive_aaaa > 0)
)

insert overwrite
table prod_features_liveinternet.url_fragment_target_cnt partition ( ymd )
select url_fragment
     , cnt_total
     , map('tinkoff_platinum_complete_application03_1m', cast(coalesce(cnt_positive_tpca, 0) as int),
           'tinkoff_platinum_approved_application03_1m', cast(coalesce(cnt_positive_tpaa, 0) as int),
           'all_airlines_complete_application03_1m', cast(coalesce(cnt_positive_aaca, 0) as int),
           'all_airlines_approved_application03_1m', cast(coalesce(cnt_positive_aaaa, 0) as int)
           ) as cnt_positive
     , map('tinkoff_platinum_complete_application03_1m', log((cnt_positive_tpca + 0.1) / (cnt_total - cnt_positive_tpca + 0.1)),
           'tinkoff_platinum_approved_application03_1m', log((cnt_positive_tpaa + 0.1) / (cnt_total - cnt_positive_tpaa + 0.1)),
           'all_airlines_complete_application03_1m', log((cnt_positive_aaca + 0.1) / (cnt_total - cnt_positive_aaca + 0.1)),
           'all_airlines_approved_application03_1m', log((cnt_positive_aaaa + 0.1) / (cnt_total - cnt_positive_aaaa + 0.1))
           ) as score
     , current_timestamp as load_dttm
     , "${hiveconf:CALC_LAST_DATE}" as ymd
  from t
; 
