
import datetime
from datetime import date, timedelta
import sys

ymd_to_process = datetime.datetime.strptime(sys.argv[1],'%Y-%m-%d')

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt update started for {1}.'.format(datetime.datetime.now(),ymd_to_process))

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
init_query = '''
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
'''

query_pattern = '''

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
                where ymd between "#ymd0" and date_add("#ymd0",3)
                  and action_type in ('tinkoff_platinum_complete_application', 'tinkoff_platinum_approved_application', 'all_airlines_complete_application', 'all_airlines_approved_application')
               ) ua on ua.id = vf.id
        where vf.ymd = "#ymd0"
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
     , "#ymd0" as ymd
  from t

'''


ymd0      = ymd_to_process.strftime('%Y-%m-%d')
ymd_start = (ymd_to_process + datetime.timedelta(days = 0)).strftime('%Y-%m-%d')
ymd_end   = (ymd_to_process + datetime.timedelta(days = 3)).strftime('%Y-%m-%d')
N = '03'
q = query_pattern.replace('#ymd0',ymd0).replace('#ymd_start',ymd_start).replace('#ymd_end',ymd_end)
#print(q)
cursor.execute(q)

print('{0}: prod_features_liveinternet.urlfr_tgt_cnt successfully updated. Date: {1}').format(datetime.datetime.now(),ymd_to_process)
