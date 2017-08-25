import datetime
from datetime import date, timedelta

print('{0}: prod_features_liveinternet.visits_r update started').format(datetime.datetime.now())

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

ymd_to_process = str(datetime.date.today()-datetime.timedelta(days=1))

queries = '''
insert
  into prod_features_liveinternet.visits_r partition(ymd)    
select id
     , urlfr
     , min(named_struct( 't', if((split(urlfr,'#')[0] <> ref_host) and not ref_host in ('','-','B'), time, time + 10000000)
	                   , 'ref_host', ref_host)).ref_host as ref_host
     , cast(count(*) as int) hits
     , cast(sum(hits_old) as int) hits_old
     , cast(count(distinct url) as int) as page_cnt
     , cast(MAX(time) - MIN(time) as int) as duration
     , AVG(time) as avg_time
     , stddev(time) as time_std
     , max(email_visit_flag) as email_visit
     , max(email_ru_flag) as emailru
     , max(utm_flag) as utm
     , max(if(regexp_extract(ref_host,'(.*)\\\\.[a-zA-Z]*$',1) in ('google','google.co','google.com','yandex','go.mail','nova.rambler','bing','yahoo'),1, 0)) as ref_search
     , max(if(regexp_extract(ref_host,'([^.]*)\\\\.[a-zA-Z]*$',1) in ('vk','facebook','ok','odnoklassniki','instagram','youtube'),1, 0)) as ref_social
     , max(ip) as ip
     , count(distinct ip) as ip_cnt
     , max(current_timestamp()) as load_dttm
     , ymd
  from(select id
            , time
            , concat(parse_url(concat('http://', url), 'HOST'), '#', path_fr) as urlfr
            , ymd
            , if(lower(concat(url,' ',referrer)) rlike '[^a-z](utm[_-]?[a-z]{0,15}|from|source|src(id)?)[%a-f0-9]{0,4}[=-_]?e?mail',1, 0) as email_visit_flag
            , if(lower(referrer) rlike 'e\\\\.mail\\\\.ru',1, 0) as email_ru_flag
            , if(lower(concat(url,' ',referrer)) rlike '([^a-zA-Z]utm(_|-|=|%[0-9][0-9a-fA-F]))|/?[yg]clid=',1, 0) as utm_flag
            , url
            , parse_url(concat('http://', referrer), 'HOST') as ref_host
            , hits_old
            , ip
         from(select id
                   , avg(cast(timestamp as Bigint)) as time
                   , url
                   , referrer
                   , ymd
                   , ip
                   , count(*) as hits_old
                from prod_raw_liveinternet.access_log
               where ymd = '#ymd'
               group
                  by id
				   , url
				   , referrer,ymd,ip,cast(timestamp/10 as Bigint)
            ) a
      LATERAL
         VIEW explode(split(parse_url(concat('http://', url), "PATH"), '/')) tt AS path_fr
     ) t
 group
    by ymd
	 , id
     , urlfr
;

alter table prod_features_liveinternet.visits_r partition (ymd = '#ymd') concatenate

'''.replace('#ymd',ymd_to_process)

#print 'Query to execute: ' + str(query)
for q in queries.split(';'):
    cursor.execute(q)

print('{0}: prod_features_liveinternet.visits_r successfully updated. Date: {1}').format(datetime.datetime.now(),ymd_to_process)
#from subprocess import call
#call('touch /home/bigdatasys/projects/prod_features_liveinternet/visits_r.txt', shell = True)

# TODO update prod_features_liveinternet.visits