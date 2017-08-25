import datetime, sys, re
from datetime import date, timedelta

print('\n' + '*' * 40 + '\n{0}: Update of user_action started.'.format(datetime.datetime.now()))
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
clean_after_self = True
from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
cursor = conn.cursor()

cursor.execute('select coalesce(max(ymd), "2015-12-31") from prod_features_liveinternet.user_action where CAST(ymd AS Date) IS NOT NULL')
try:
    last_upd_ymd = cursor.fetchone()[0]
except IndexError:
    print('Error connecting to prod_features_liveinternet')
    exit(1)    

ymd_start = str(datetime.datetime.strptime(last_upd_ymd, "%Y-%m-%d").date()+datetime.timedelta(days=1))
ymd_end = str(datetime.date.today()-datetime.timedelta(days=2))

ind = ''.join(re.findall(r"[\d']+", str(datetime.datetime.now())))[:14]

last_upd_ymd_dt = datetime.datetime.strptime(last_upd_ymd, "%Y-%m-%d").date()

if(date.today() - last_upd_ymd_dt <= datetime.timedelta(days=2)):
    print('User_action is up to date')
    sys.exit(0)


queries = '''
	drop table if exists prod_features_liveinternet.cc_wuid_#ind;
    create table prod_features_liveinternet.cc_wuid_#ind as
    select
        min(a.dt_created) as dt_created
		,a.ymd
		,a.wuid
		,max(1) as completed_flag
		,max(0) as revisited
        ,case when max(c.decision_approve_flg) = 'Y' then 1 else 0 end as decision_approve_flg
        ,case when a.product_name = 'cc_allairlines' 
             or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
             then 'AA' else 'Platinum' end product
        ,count(*) as apps
    from
		prod_dds.portal_application a
		left join hermes.platinum_lp b on a.lp = b.lp
        inner join prod_emart.financial_account_application c on a.hid = c.hid 
    where 
		a.ymd between '#ymd_start' and '#ymd_end'
    	and a.wuid is not null 
		and
         (
          (
	 		(
			 (
			  a.product_name = 'cc_platinum' and a.lp not in ('/credit/formliteagent/loan/', '/cards/credit-cards/cobrands/auramall/agent/form/short/', '/mgm/form/')
			 )
			 or b.lp is not null
			)
    	and lower(a.lp) not like '%agent%'
        and a.agent_code is null
          ) 
          or
          (
           (
            a.product_name = 'cc_allairlines' or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
            )
         and lower(a.lp) not like '%agent%'
         and a.agent_code is null
          
          ) 
         ) 
    	and a.is_processed = 3
        and a.linked_id is null
    group by 
        a.ymd
		,a.wuid
        ,case when a.product_name = 'cc_allairlines' 
             or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
             then 'AA' else 'Platinum' end
;
insert into prod_features_liveinternet.cc_wuid_#ind
    select
        min(s.dt_created) as dt_created
        ,cast(s.dt_created as date) as ymd
        ,f.wuid
    	,max(1) as completed_flag
    	,max(1) as revisited
        ,case when max(s.decision_approve_flg) = 'Y' then 1 else 0 end as decision_approve_flg
        ,max(s.product) product
        ,count(*) apps
    from 
		(select
			a.id
			,a.linked_id
            ,c.crm_income_dttm as dt_created
            ,c.decision_approve_flg
            ,case when a.product_name = 'cc_allairlines' 
             or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
             then 'AA' else 'Platinum' end product
		from
			prod_dds.portal_application a
			left join hermes.platinum_lp b on a.lp = b.lp
            inner join prod_emart.financial_account_application c on a.hid = c.hid
	    where    
			is_processed = 3             
            and
             (              
                (
                 (
                  a.product_name = 'cc_platinum' and a.lp not in ('/credit/formliteagent/loan/', '/cards/credit-cards/cobrands/auramall/agent/form/short/', '/mgm/form/')
                 )
                 or b.lp is not null
                )              
              or
               (
                a.product_name = 'cc_allairlines' or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
                ) 
             )  
            and not linked_id is null
            and cast(c.crm_income_dttm as date) between '#ymd_start' and '#ymd_end'
	    ) s
    	inner join 
		(
		select
			id
			,dt_created
			,ymd
			,wuid
		from 
			prod_dds.portal_application a
			left join hermes.platinum_lp b on a.lp = b.lp            
		where    
			a.is_processed = 21
            and a.wuid is not null
	        and lower(a.lp) not like '%agent%'
            and a.agent_code is null
	        and
             (
              (
                (
                 (
                  a.product_name = 'cc_platinum' and a.lp not in ('/credit/formliteagent/loan/', '/cards/credit-cards/cobrands/auramall/agent/form/short/', '/mgm/form/')
                 )
                 or b.lp is not null
                )
              )
              or
              (
               
                a.product_name = 'cc_allairlines' or a.lp in ('/cards/credit-cards/all-airlines/','/credit/cards/allairlines/','/credit/form/aa/allairlinesdiscounts/','/credit/form/mgmallairlines/')
                
              ) 
             )
		) f on s.linked_id = f.id		
    group by
         cast(s.dt_created as date)
        ,f.wuid
    ;
drop table if exists prod_features_liveinternet.cc_wuid_li_#ind;
    create table prod_features_liveinternet.cc_wuid_li_#ind as
    select
        a.ymd
        ,a.dt_created
        ,a.wuid
        ,a.completed_flag
        ,a.revisited
        ,a.decision_approve_flg
        ,a.product
        ,b.dmp_id
        ,c.source_id as li_id
    from
     prod_features_liveinternet.cc_wuid_#ind a
     	inner join (select distinct source_id, dmp_id from prod_emart.datamind_matching_table where source_type = 'tcs') b on a.wuid = b.source_id
     	inner join (select distinct source_id, dmp_id from prod_emart.datamind_matching_table where source_type = 'liveinternet') c on b.dmp_id = c.dmp_id
    ;
	drop table if exists prod_features_liveinternet.cc_wuid_li_filtered_#ind;
	create table prod_features_liveinternet.cc_wuid_li_filtered_#ind as
	select
	 a.*
	from
	 prod_features_liveinternet.cc_wuid_li_#ind a
	 left semi join
	  (
		select
		 wuid
		 ,count(distinct li_id) li_ids
		from
		 prod_features_liveinternet.cc_wuid_li_#ind
		group by
		 wuid
		having
		 count(distinct li_id) <= 3     
		) b on a.wuid = b.wuid
    ;
	insert overwrite
		table prod_features_liveinternet.user_action partition(action_type, ymd)
	select
		li_id as id
		,dt_created as time
        ,current_timestamp() as load_dttm
		,'tinkoff_platinum_complete_application' as action_type
		,ymd
	from
		prod_features_liveinternet.cc_wuid_li_filtered_#ind
    where 
        product = 'Platinum'
	;
	insert overwrite
		table prod_features_liveinternet.user_action partition(action_type, ymd)
	select
		li_id as id
		,dt_created as time
        ,current_timestamp() as load_dttm
		,'all_airlines_complete_application' as action_type
		,ymd
	from
		prod_features_liveinternet.cc_wuid_li_filtered_#ind
    where 
        product = 'AA'
	;
    insert overwrite
		table prod_features_liveinternet.user_action partition(action_type, ymd)
	select
		li_id as id
		,dt_created as time
        ,current_timestamp() as load_dttm
		,'tinkoff_platinum_approved_application' as action_type
		,ymd
	from
		prod_features_liveinternet.cc_wuid_li_filtered_#ind
	where
		decision_approve_flg = 1
        and product = 'Platinum'        
	;
    insert overwrite
		table prod_features_liveinternet.user_action partition(action_type, ymd)
	select
		li_id as id
		,dt_created as time
        ,current_timestamp() as load_dttm
		,'all_airlines_approved_application' as action_type
		,ymd
	from
		prod_features_liveinternet.cc_wuid_li_filtered_#ind
	where
		decision_approve_flg = 1
        and product = 'AA'
	;

'''.replace('#ind',ind).replace('#ymd_start',ymd_start).replace('#ymd_end',ymd_end)

query_list = queries.split(';')[:-1]

for q in query_list: cursor.execute(q)

if(clean_after_self): 
    for q in query_list: 
        if('drop table ' in q): cursor.execute(q)

print('{0}: User_action successfully updated. Dates: ' + ymd_start + '---' + ymd_end).format(datetime.datetime.now())