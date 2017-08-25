#
# coding: utf-8

from pyhive import hive
import os
import sys
from subprocess import call
import datetime
from datetime import date, timedelta
from time import sleep


# In[ ]:

def visits_ready():
    res = os.path.exists('/home/bigdatasys/projects/prod_features_liveinternet/visits_ready')
    return res


# In[5]:

HIVE_HOST = 'ds-hadoop-cs01p'
HIVE_PORT = 10000
HIVE_USER = 'bigdatasys'
CONF1={'hive.vectorized.execution.enabled':'true'
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
    ,'mapred.output.compression.codec':'org.apache.hadoop.io.compress.BZip2Codec'
    ,'mapreduce.map.failures.maxpercent':'5'
          }

CONF2={'hive.vectorized.execution.enabled':'true'
    ,'mapreduce.map.memory.mb':'4096'
    ,'mapreduce.map.child.java.opts':'-Xmx4g'
    ,'mapreduce.task.io.sort.mb':'1024'
    ,'mapreduce.reduce.child.java.opts':'-Xmx4g'
    ,'mapreduce.reduce.memory.mb':'7000'
    ,'mapreduce.reduce.shuffle.input.buffer.percent':'0.5'
    ,'mapreduce.input.fileinputformat.split.minsize':'61560725'
    ,'mapreduce.input.fileinputformat.split.maxsize':'123121450'
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
    ,'mapred.output.compression.codec':'org.apache.hadoop.io.compress.BZip2Codec'
    ,'mapreduce.map.failures.maxpercent':'5'
          }


conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF1)
cursor = conn.cursor()

try:
    call('rm /home/bigdatasys/projects/la_segments_builder/data/*', shell = True)
except:
    pass

# In[6]:

ymd_start = str(date.today() - timedelta(3))
ymd_end = str(date.today() - timedelta(1))
ymd_current = str(date.today())
ind = ymd_end.replace('-', '')

print('*' * 30 + '\nXGB for {0} started at {1}.\n'.format(ymd_end, datetime.datetime.now()))


# In[7]:

print ymd_start, ymd_end, ymd_current, ind


# In[18]:

queries_indep = '''
drop table if exists prod_lookalike.boo_#ind_0;
create table prod_lookalike.boo_#ind_0 as
select distinct
 id
from
 prod_raw_liveinternet.access_log
where
 ymd = "#ymd_end"
;

drop table if exists prod_lookalike.boo_#ind_1;
create table prod_lookalike.boo_#ind_1 as
select
 a.id
 ,b.h_uid_rk
from
 prod_lookalike.boo_#ind_0 a
 inner join prod_dds.h_uid b on a.id = b.uid_str
 left semi join prod_dds.l_uid_contact c on b.h_uid_rk = c.h_uid_rk and c.contact_type_cd = 'EMAIL' 
where
 b.load_src = 'LI.02'
'''

queries_dep = '''
drop table if exists prod_lookalike.boo_#ind_2;
create table prod_lookalike.boo_#ind_2 as
select
 a.h_uid_rk
 ,b.urlfr
 ,b.avg_hour
 ,b.cnt
 ,case when c.segment_nm = 'tcs_cc_complete_app_1m_3days_3_5000' then c.score end as coeff
 ,case when c.segment_nm = 'tcs_email_cc_completed_app_longtime_10and50' then c.score end as email_coeff
 ,(unix_timestamp("#ymd_current", 'yyyy-MM-dd') - unix_timestamp(b.ymd, 'yyyy-MM-dd'))/60/60/24 as lag
from
 prod_lookalike.boo_#ind_1 a
 inner join prod_features_liveinternet.visits b on a.id = b.id
 left join (select * from prod_lookalike.urlfr_coeff where segment_nm in ('tcs_cc_complete_app_1m_3days_3_5000', 'tcs_email_cc_completed_app_longtime_10and50')) c on b.urlfr = c.urlfr
where
 b.ymd between "#ymd_start" and "#ymd_end"
;

drop table if exists prod_lookalike.boo_#ind_3;
create table prod_lookalike.boo_#ind_3 as
select
h_uid_rk
,max(coeff) as max_score
,min(coeff) as min_score
,avg(coeff) as avg_score
,percentile_approx(coeff,0.90) as p90
,percentile_approx(coeff,0.75) as p75
,percentile_approx(coeff,0.50) as p50
,percentile_approx(coeff,0.25) as p25
,count(distinct urlfr) as urlfrs
,count(distinct substr(urlfr, 0, instr(urlfr,'#')-1)) as domains
,count(distinct case when coeff > -7 then urlfr end) as good_urlfr 
,sum(cnt) as hits
,sum(if(urlfr like 'e.mail.ru%',1,0)) as emailru
,sum(if(urlfr like 'm.%',1,0))/sum(1) as mobile_share
,sum(if(urlfr rlike 'vk\\.com',1, 0))/sum(1) as vk_share
,sum(if(urlfr rlike '^(m\\.)?ok\\.ru' or urlfr rlike 'm\\.odnoklassniki\\.ru',1,0))/sum(1) as ok_share
,stddev(coeff) as stddev_score
,stddev(lag) as stddev_lag
,max(lag) as max_lag
,min(lag) as min_lag
,count(distinct lag) lags
,min(avg_hour) as min_avg_hour
,max(avg_hour) as max_avg_hour
,avg(avg_hour) as avg_avg_hour
,max(case when lag = 1 then coeff end)/max(case when lag = 2 then coeff end) as max_lag1_to_max_lag2
,max(case when lag = 1 then coeff end)/max(case when lag = 3 then coeff end) as max_lag1_to_max_lag3
,max(case when lag = 2 then coeff end)/max(case when lag = 3 then coeff end) as max_lag2_to_max_lag3
,max(case when lag = 1 then coeff end) as max_score_lag1
,min(case when lag = 1 then coeff end) as min_score_lag1
,avg(case when lag = 1 then coeff end) as avg_score_lag1
,percentile_approx(case when lag = 1 then coeff end,0.90) as p90_lag1
,percentile_approx(case when lag = 1 then coeff end,0.75) as p75_lag1
,percentile_approx(case when lag = 1 then coeff end,0.50) as p50_lag1
,percentile_approx(case when lag = 1 then coeff end,0.25) as p25_lag1
,max(case when lag = 2 then coeff end) as max_score_lag2
,min(case when lag = 2 then coeff end) as min_score_lag2
,avg(case when lag = 2 then coeff end) as avg_score_lag2
,percentile_approx(case when lag = 2 then coeff end,0.90) as p90_lag2
,percentile_approx(case when lag = 2 then coeff end,0.75) as p75_lag2
,percentile_approx(case when lag = 2 then coeff end,0.50) as p50_lag2
,percentile_approx(case when lag = 2 then coeff end,0.25) as p25_lag2
,max(case when lag = 3 then coeff end) as max_score_lag3
,min(case when lag = 3 then coeff end) as min_score_lag3
,avg(case when lag = 3 then coeff end) as avg_score_lag3
,percentile_approx(case when lag = 3 then coeff end,0.90) as p90_lag3
,percentile_approx(case when lag = 3 then coeff end,0.75) as p75_lag3
,percentile_approx(case when lag = 3 then coeff end,0.50) as p50_lag3
,percentile_approx(case when lag = 3 then coeff end,0.25) as p25_lag3 
,max(email_coeff) as max_email_score
,min(email_coeff) as min_email_score
,avg(email_coeff) as avg_email_score
,percentile_approx(email_coeff,0.90) as p90_email_email_score
,percentile_approx(email_coeff,0.75) as p75_email_email_score
,percentile_approx(email_coeff,0.50) as p50_email_email_score
,percentile_approx(email_coeff,0.25) as p25_email_email_score
,stddev(email_coeff) as stddev_email_score
,max(case when lag = 1 then email_coeff end) as max_email_score_lag1
,min(case when lag = 1 then email_coeff end) as min_email_score_lag1
,avg(case when lag = 1 then email_coeff end) as avg_email_score_lag1
,percentile_approx(case when lag = 1 then email_coeff end,0.90) as p90_email_score_lag1
,percentile_approx(case when lag = 1 then email_coeff end,0.75) as p75_email_score_lag1
,percentile_approx(case when lag = 1 then email_coeff end,0.50) as p50_email_score_lag1
,percentile_approx(case when lag = 1 then email_coeff end,0.25) as p25_email_score_lag1
,max(case when lag = 2 then email_coeff end) as max_email_score_lag2
,min(case when lag = 2 then email_coeff end) as min_email_score_lag2
,avg(case when lag = 2 then email_coeff end) as avg_email_score_lag2
,percentile_approx(case when lag = 2 then email_coeff end,0.90) as p90_email_score_lag2
,percentile_approx(case when lag = 2 then email_coeff end,0.75) as p75_email_score_lag2
,percentile_approx(case when lag = 2 then email_coeff end,0.50) as p50_email_score_lag2
,percentile_approx(case when lag = 2 then email_coeff end,0.25) as p25_email_score_lag2
,max(case when lag = 3 then email_coeff end) as max_email_score_lag3
,min(case when lag = 3 then email_coeff end) as min_email_score_lag3
,avg(case when lag = 3 then email_coeff end) as avg_email_score_lag3
,percentile_approx(case when lag = 3 then email_coeff end,0.90) as p90_email_score_lag3
,percentile_approx(case when lag = 3 then email_coeff end,0.75) as p75_email_score_lag3
,percentile_approx(case when lag = 3 then email_coeff end,0.50) as p50_email_score_lag3
,percentile_approx(case when lag = 3 then email_coeff end,0.25) as p25_email_score_lag3
from
 prod_lookalike.boo_#ind_2
group by
 h_uid_rk
;

drop table if exists prod_lookalike.boo_#ind_4_features;
create table prod_lookalike.boo_#ind_4_features as
select
 concat_ws(' ', concat_ws('1:','',cast(max_score as string)),	concat_ws('2:','',cast(min_score as string)),	concat_ws('3:','',cast(avg_score as string))
            ,	concat_ws('4:','',cast(p90 as string)),	concat_ws('5:','',cast(p75 as string)),	concat_ws('6:','',cast(p50 as string)),	concat_ws('7:','',cast(p25 as string))
            ,	concat_ws('8:','',cast(urlfrs as string)),	concat_ws('9:','',cast(domains as string)),	concat_ws('10:','',cast(good_urlfr as string))
            ,	concat_ws('11:','',cast(hits as string)),	concat_ws('12:','',cast(emailru as string)),	concat_ws('13:','',cast(mobile_share as string))
            ,	concat_ws('14:','',cast(vk_share as string)),	concat_ws('15:','',cast(ok_share as string)),	concat_ws('16:','',cast(stddev_score as string))
            ,	concat_ws('17:','',cast(stddev_lag as string)),	concat_ws('18:','',cast(max_lag as string)),	concat_ws('19:','',cast(min_lag as string))
            ,	concat_ws('20:','',cast(lags as string)),	concat_ws('21:','',cast(min_avg_hour as string)),	concat_ws('22:','',cast(max_avg_hour as string))
            ,	concat_ws('23:','',cast(avg_avg_hour as string)),	concat_ws('24:','',cast(max_lag1_to_max_lag2 as string)),	concat_ws('25:','',cast(max_lag1_to_max_lag3 as string))
            ,	concat_ws('26:','',cast(max_lag2_to_max_lag3 as string)),	concat_ws('27:','',cast(max_score_lag1 as string)),	concat_ws('28:','',cast(min_score_lag1 as string))
            ,	concat_ws('29:','',cast(avg_score_lag1 as string)),	concat_ws('30:','',cast(p90_lag1 as string)),	concat_ws('31:','',cast(p75_lag1 as string))
            ,	concat_ws('32:','',cast(p50_lag1 as string)),	concat_ws('33:','',cast(p25_lag1 as string))
            ,	concat_ws('34:','',cast(max_score_lag2 as string)),	concat_ws('35:','',cast(min_score_lag2 as string))
            ,	concat_ws('36:','',cast(avg_score_lag2 as string)),	concat_ws('37:','',cast(p90_lag2 as string)),	concat_ws('38:','',cast(p75_lag2 as string))
            ,	concat_ws('39:','',cast(p50_lag2 as string)),	concat_ws('40:','',cast(p25_lag2 as string)),	concat_ws('41:','',cast(max_score_lag3 as string))
            ,	concat_ws('42:','',cast(min_score_lag3 as string)),	concat_ws('43:','',cast(avg_score_lag3 as string)),	concat_ws('44:','',cast(p90_lag3 as string))
            ,	concat_ws('45:','',cast(p75_lag3 as string)),	concat_ws('46:','',cast(p50_lag3 as string)),	concat_ws('47:','',cast(p25_lag3 as string))
            ,	concat_ws('48:','',cast(max_email_score as string)),	concat_ws('49:','',cast(min_email_score as string)),	concat_ws('50:','',cast(avg_email_score as string))
            ,	concat_ws('51:','',cast(p90_email_email_score as string)),	concat_ws('52:','',cast(p75_email_email_score as string)),	concat_ws('53:','',cast(p50_email_email_score as string))
            ,   concat_ws('54:','',cast(p25_email_email_score as string)),	concat_ws('55:','',cast(stddev_email_score as string)),	concat_ws('56:','',cast(max_email_score_lag1 as string))
            ,	concat_ws('57:','',cast(min_email_score_lag1 as string)),	concat_ws('58:','',cast(avg_email_score_lag1 as string)),	concat_ws('59:','',cast(p90_email_score_lag1 as string))
            ,	concat_ws('60:','',cast(p75_email_score_lag1 as string)),	concat_ws('61:','',cast(p50_email_score_lag1 as string)),	concat_ws('62:','',cast(p25_email_score_lag1 as string))
            ,	concat_ws('63:','',cast(max_email_score_lag2 as string)),	concat_ws('64:','',cast(min_email_score_lag2 as string)),	concat_ws('65:','',cast(avg_email_score_lag2 as string))
            ,	concat_ws('66:','',cast(p90_email_score_lag2 as string)),	concat_ws('67:','',cast(p75_email_score_lag2 as string)),	concat_ws('68:','',cast(p50_email_score_lag2 as string))
            ,	concat_ws('69:','',cast(p25_email_score_lag2 as string)),	concat_ws('70:','',cast(max_email_score_lag3 as string)),	concat_ws('71:','',cast(min_email_score_lag3 as string))
            ,	concat_ws('72:','',cast(avg_email_score_lag3 as string)),	concat_ws('73:','',cast(p90_email_score_lag3 as string)),	concat_ws('74:','',cast(p75_email_score_lag3 as string))
            ,	concat_ws('75:','',cast(p50_email_score_lag3 as string)),	concat_ws('76:','',cast(p25_email_score_lag3 as string))
            ) as label_features          
 from
  prod_lookalike.boo_#ind_3
;
drop table if exists prod_lookalike.boo_#ind_4_huids;
create table prod_lookalike.boo_#ind_4_huids as
select
 h_uid_rk        
 from
  prod_lookalike.boo_#ind_3
'''


# In[19]:
i = 0

for query in queries_indep.split(';'):
    print '\nQuery {0}:'.format(str(i))
    print query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current)
    print('Query {0} started at {1}.'.format(str(i), datetime.datetime.now()))
    sys.stdout.flush()
    cursor.execute(query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current))
    print('Query {0} finished at {1}.'.format(str(i), datetime.datetime.now()))
    sys.stdout.flush()
    i += 1

# In[20]:

while not visits_ready():
    sleep(300)

for query in queries_dep.split(';'):
    print '\nQuery {0}:'.format(str(i))
    #print query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current)
    print('Query {0} started at {1}.'.format(str(i), datetime.datetime.now()))
    sys.stdout.flush()

    if 'create table prod_lookalike.boo_#ind_3' in query:
        cursor.close()
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF2)
        cursor = conn.cursor()

    cursor.execute(query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current))
    print('Query {0} finished at {1}.'.format(str(i), datetime.datetime.now()))
    sys.stdout.flush()
    #cursor.close()
    #conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF1)
    #cursor = conn.cursor()
    i += 1



# In[ ]:

os.chdir('/home/bigdatasys/projects/la_segments_builder/data')

print('Download started at {0}.'.format(datetime.datetime.now()))
sys.stdout.flush()

call('hadoop fs -text /prod_lookalike/boo_#ind_4_features/* > ./XGBoost_features.txt'.replace('#ind',ind), shell = True)
call('hadoop fs -text /prod_lookalike/boo_#ind_4_huids/* > ./XGBoost_huids.txt'.replace('#ind',ind), shell = True)

print('Download finished at {0}.\n'.format(datetime.datetime.now()))
sys.stdout.flush()


print('Model applying started at {0}.\n'.format(datetime.datetime.now()))
sys.stdout.flush()

call('/home/bigdatasys/soft/xgboost/xgboost /home/bigdatasys/projects/la_segments_builder/cc_emails.conf task=pred model_in=/home/bigdatasys/projects/la_segments_builder/XGB.model'
     ,shell = True)

print('Model applying finished at {0}.\n'.format(datetime.datetime.now()))
sys.stdout.flush()

call('paste -d "," XGBoost_huids.txt pred.txt > h_uid_rk_x_score.txt', shell = True)

try:
    call('hadoop fs -rm -r /prod_lookalike/boosting/*', shell = True)   
except:
    pass
call('hadoop fs -put /home/bigdatasys/projects/la_segments_builder/data/h_uid_rk_x_score.txt /prod_lookalike/boosting/h_uid_rk_x_score.txt', shell = True)


# In[10]:

cursor.execute('drop table if exists prod_lookalike.boo_#ind_5'.replace('#ind', ind))
cursor.execute(
'''
create external table prod_lookalike.boo_#ind_5 (
h_uid_rk string
,score double
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ','
	LOCATION
	  'hdfs://nameservice1/prod_lookalike/boosting'
'''.replace('#ind', ind)
)

print('\nFinal table created at {0}.\n'.format(datetime.datetime.now()))
sys.stdout.flush()

cursor.execute("""
insert overwrite
 table prod_lookalike.user_x_segment partition(segment_nm, ymd)
select
 h_uid_rk
 ,score
 ,current_timestamp() as load_dttm
 ,'email_boosting_v1' as segment_nm
 ,"#ymd_end" as ymd
from
 prod_lookalike.boo_#ind_5
order by
  score desc
limit 200000""".replace('#ind',ind).replace('#ymd_end', ymd_end)
)



call('touch /home/bigdatasys/projects/la_segments_builder/new_segments_ready', shell = True)

print('\nla_lookalike_apply for {0} SUCCESS. {1}.\n'.format(ymd_end, datetime.datetime.now()) + '*' * 30 + '\n')

call('rm /home/bigdatasys/projects/prod_features_liveinternet/visits_ready', shell = True)

try:
    call('rm /home/bigdatasys/projects/la_segments_builder/data/*', shell = True)
except:
    pass

cursor.execute('drop table if exists prod_lookalike.boo_#ind_5'.replace('#ind', ind))

for query in queries_indep.split(';'):
    if 'drop table' in query:
        cursor.execute(query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current))
    
for query in queries_dep.split(';'):
    if 'drop table' in query:
        cursor.execute(query.replace('#ind', ind).replace('#ymd_start', ymd_start).replace('#ymd_end', ymd_end).replace('#ymd_current', ymd_current))
    
cursor.close()