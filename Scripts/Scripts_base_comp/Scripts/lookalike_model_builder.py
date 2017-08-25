
# coding: utf-8

# In[1]:

def lift(data): #data - list of tuples (y1_lables, score) sorted by score desc
    import math
    import pandas as pd
    n = len(data)
    total1 = sum([x[0] for x in data])
    total2 = len(data)
    avg1 = 1.0*total1/n
    print('Avg p1 = %f' % avg1)
    k = 1000
    step = math.floor(n/k)
    res = []
    cum_sum1 = 0
    cum_sum2 = 0
    for i in xrange(k):
        i_sum1 = sum([x[0] for x in data[(i*n/k):((i+1)*n/k)]])
        i_sum2 = len(data[(i*n/k):((i+1)*n/k)])
        cum_sum1 += i_sum1
        cum_sum2 += i_sum2
        # str((float(i)*100/k)) + '-' + str(
        res.append((  (float(i)+1)*100/k, 1.0*i_sum1*k/n/avg1, data[(i+1)*n/k-1][1], i_sum1, 100.0*i_sum1/total1, 1.0*i_sum1*k/n,                     cum_sum1, 100.0*cum_sum1/total1, cum_sum2))
    return pd.DataFrame(res, columns = ('%', 'lift', 'min_score', 'cnt', '% of total pos', 'p', 'cumsum', 'cumsum %', 'total_cumsum'))


# In[6]:

def build_la_model(cluster_prod, HIVE_USER, ymd_start, ymd_end, ymd_test, tinkoff_page, target_url_like_expression, domain_exclude, seg_name, flag_file, unique_sync = False):    

    import time
    start_time = time.time()
    # In[1]:

    from pyhive import hive
    from string import Template
    import pandas as pd
    
    if cluster_prod == True:
        HIVE_HOST = 'ds-hadoop-cs01p'
    else:
        HIVE_HOST = 'm1-hadoop-cs01t'     
    HIVE_PORT = 10000
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


    # In[2]:

    # In[4]:

    import datetime
    import re
    import sys
    now = datetime.datetime.now()

    

    calc_id = ''.join(re.findall(r"[\d']+", str(now)))[:14]


    # In[5]:

    
    tmp_tables = {'positive_objects':'prod_ex_machina.tmp_lookalike_seg_name_pos_obj_calc_id'
                  ,'logs_with_flags':'prod_ex_machina.tmp_lookalike_seg_name_logs_flags_calc_id'
                  ,'domain_group_freqs':'prod_ex_machina.tmp_lookalike_seg_name_domain_calc_id'
                  ,'lev0_group_freqs':'prod_ex_machina.tmp_lookalike_seg_name_lev0_calc_id'
                  ,'lev1_group_freqs':'prod_ex_machina.tmp_lookalike_seg_name_lev1_calc_id'
                  ,'lev2_group_freqs':'prod_ex_machina.tmp_lookalike_seg_name_lev2_calc_id'
                  ,'scores':'prod_ex_machina.tmp_lookalike_seg_name_scores_calc_id'
                  ,'positive_objects_t':'prod_ex_machina.tmp_lookalike_seg_name_pos_obj_t_calc_id'
                  ,'logs_with_flags_t':'prod_ex_machina.tmp_lookalike_seg_name_logs_flags_t_calc_id'
                  ,'up_logs_t':'prod_ex_machina.tmp_lookalike_seg_name_up_logs_t_calc_id'
                  ,'flags_x_scores_t':'prod_ex_machina.tmp_lookalike_seg_name_flags_x_scores_t_calc_id'
                  ,'lift':'prod_ex_machina.tmp_lookalike_seg_name_lift_calc_id'
                  }

    # In[6]:

    tmp_tables_cust = dict((x, tmp_tables[x].replace('calc_id',calc_id).replace('seg_name',seg_name)) for x in tmp_tables.keys())
    print 'Будут созданы таблицы:'
    print '\n'.join(tmp_tables_cust.values())
    sys.stdout.flush()

    # In[7]:
    
    if tinkoff_page == True:
	if unique_sync == False:
            positive_objects_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects']
	    positive_objects_q_create = 'create table ' + tmp_tables_cust['positive_objects'] + """ as
            select distinct
                concat(d.source_id, "-", a.ymd) as object_id
            from
                (select distinct 
                    h_uid_rk
                    ,ymd 
                from 
                    prod_rdds.r_visit
                where
                    ymd between "ymd_start" and "ymd_end"
                    and target_url_like_expression
                    and load_src = 'PRT.01'
                ) a
                inner join prod_dds.h_uid b on a.h_uid_rk = b.h_uid_rk and b.load_src = 'PRT.01'
                inner join prod_emart.dm_webuser_sync c on b.uid_str = c.source_id and c.source_type = 'tcs' and length(c.dmp_id) > 0
                inner join prod_emart.dm_webuser_sync d on c.dmp_id = d.dmp_id and d.source_type = 'liveinternet'  
            """.replace('ymd_start', ymd_start) \
               .replace('ymd_end', ymd_end) \
               .replace('target_url_like_expression', target_url_like_expression)
        else:
            positive_objects_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects']
	    positive_objects_q_create = 'create table ' + tmp_tables_cust['positive_objects'] + """ as
            select
             object_id
            from
            (
             select
                concat(d.source_id, "-", a.ymd) as object_id
                ,d.source_id
                ,a.h_uid_rk
                ,count(*) over (partition by a.h_uid_rk) prt_rows_cnt
                ,count(*) over (partition by d.source_id) li_rows_cnt
             from
                (select distinct 
                 h_uid_rk
                 ,ymd 
                 from 
                 prod_rdds.r_visit
                 where 
                  ymd between "ymd_start" and "ymd_end"
                  and target_url_like_expression
                  and load_src = 'PRT.01'
                ) a
                inner join prod_dds.h_uid b on a.h_uid_rk = b.h_uid_rk and b.load_src = 'PRT.01'
                inner join prod_emart.dm_webuser_sync c on b.uid_str = c.source_id and c.source_type = 'tcs' and length(c.dmp_id) > 0
                inner join prod_emart.dm_webuser_sync d on c.dmp_id = d.dmp_id and d.source_type = 'liveinternet'
            ) t
            where 
             prt_rows_cnt = datediff("ymd_end", "ymd_start") + 1
             and li_rows_cnt = datediff("ymd_end", "ymd_start") + 1           
            """.replace('ymd_start', ymd_start) \
               .replace('ymd_end', ymd_end) \
               .replace('target_url_like_expression', target_url_like_expression)

    else:
        positive_objects_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects']
        positive_objects_q_create = 'create table ' + tmp_tables_cust['positive_objects'] + """ as
        select distinct 
            concat(id, "-", ymd) as object_id
        from
            prod_raw_liveinternet.access_log
        where
            ymd between "ymd_start" and "ymd_end"
            and url like "target_url_like_expression"
        """.replace('ymd_start', ymd_start) \
           .replace('ymd_end', ymd_end) \
           .replace('target_url_like_expression', target_url_like_expression)

   
    # In[8]:

    logs_with_flags_q_drop = 'drop table if exists ' + tmp_tables_cust['logs_with_flags']
    logs_with_flags_q_create = 'create table ' + tmp_tables_cust['logs_with_flags'] + """ as
    select
    concat(a.id, "-", a.ymd) as object_id
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)', 1) as domain
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?', 2) lev0
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?/?([^/]*)?', 3) lev1
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?/?([^/]*)?/?([^/]*)?', 4) lev2
    ,url
    ,case when b.object_id is not null then 1 else 0 end as positive_flag
    from
     prod_raw_liveinternet.access_log a
     left join positive_objects b on concat(a.id, "-", a.ymd) = b.object_id
    where
     a.ymd between "ymd_start" and "ymd_end"
    """.replace('ymd_start', ymd_start) \
       .replace('ymd_end', ymd_end) \
       .replace('positive_objects', tmp_tables_cust['positive_objects'])


    # In[9]:

    domain_group_freqs_q_drop = 'drop table if exists ' + tmp_tables_cust['domain_group_freqs']
    domain_group_freqs_q_create = 'create table ' + tmp_tables_cust['domain_group_freqs'] + """ as
    select
     domain as up
     ,count(distinct object_id) as total_objects
     ,count(distinct case when positive_flag = 1 then object_id end) as positive_objects
     ,count(distinct case when positive_flag = 0 then object_id end) as negative_objects
    from
     logs_with_flags
    where
     length(domain) > 0
    group by 
     domain 
    """.replace('logs_with_flags', tmp_tables_cust['logs_with_flags'])


    # In[10]:

    lev0_group_freqs_q_drop = 'drop table if exists ' + tmp_tables_cust['lev0_group_freqs']
    lev0_group_freqs_q_create = 'create table ' + tmp_tables_cust['lev0_group_freqs'] + """ as
    select
     concat(domain, "[0]", lev0) as up
     ,count(distinct object_id) as total_objects
     ,count(distinct case when positive_flag = 1 then object_id end) as positive_objects
     ,count(distinct case when positive_flag = 0 then object_id end) as negative_objects
    from
     logs_with_flags
    where
     length(lev0) > 0
    group by 
     concat(domain, "[0]", lev0)
    """.replace('logs_with_flags', tmp_tables_cust['logs_with_flags'])


    # In[11]:

    lev1_group_freqs_q_drop = 'drop table if exists ' + tmp_tables_cust['lev1_group_freqs']
    lev1_group_freqs_q_create = 'create table ' + tmp_tables_cust['lev1_group_freqs'] + """ as
    select
     concat(domain, "[1]", lev1) as up
     ,count(distinct object_id) as total_objects
     ,count(distinct case when positive_flag = 1 then object_id end) as positive_objects
     ,count(distinct case when positive_flag = 0 then object_id end) as negative_objects
    from
     logs_with_flags
    where
     length(lev1) > 0
    group by 
     concat(domain, "[1]", lev1)
    """.replace('logs_with_flags', tmp_tables_cust['logs_with_flags'])


    # In[12]:

    lev2_group_freqs_q_drop = 'drop table if exists ' + tmp_tables_cust['lev2_group_freqs']
    lev2_group_freqs_q_create = 'create table ' + tmp_tables_cust['lev2_group_freqs'] + """ as
    select
     concat(domain, "[2]", lev2) as up
     ,count(distinct object_id) as total_objects
     ,count(distinct case when positive_flag = 1 then object_id end) as positive_objects
     ,count(distinct case when positive_flag = 0 then object_id end) as negative_objects
    from
     logs_with_flags
    where
     length(lev2) > 0
    group by 
     concat(domain, "[2]", lev2)
    """.replace('logs_with_flags', tmp_tables_cust['logs_with_flags'])


    # In[13]:

    scores_q_drop = 'drop table if exists ' + tmp_tables_cust['scores']
    scores_q_create = 'create table ' + tmp_tables_cust['scores'] + """ as
    select
    *
    from
    (
    select
     up
     ,positive_objects
     ,negative_objects
     ,total_objects
     ,log((positive_objects + 0.1)/(negative_objects + 0.1)) as score
    from
     domain_group_freqs
    where
     total_objects > 1000
     or positive_objects > 5 

      union all  

    select
     up
     ,positive_objects
     ,negative_objects
     ,total_objects
     ,log((positive_objects + 0.1)/(negative_objects + 0.1)) as score
    from
     lev0_group_freqs
    where
     total_objects > 1000
     or positive_objects > 5 

      union all

    select
     up
     ,positive_objects
     ,negative_objects
     ,total_objects
     ,log((positive_objects + 0.1)/(negative_objects + 0.1)) as score
    from
     lev1_group_freqs
    where
     total_objects > 1000
     or positive_objects > 5 

      union all  

    select
     up
     ,positive_objects
     ,negative_objects
     ,total_objects
     ,log((positive_objects + 0.1)/(negative_objects + 0.1)) as score
    from
     lev2_group_freqs
    where
     total_objects > 1000
     or positive_objects > 5   
      ) t
    """.replace('domain_group_freqs', tmp_tables_cust['domain_group_freqs']) \
       .replace('lev0_group_freqs', tmp_tables_cust['lev0_group_freqs']) \
       .replace('lev1_group_freqs', tmp_tables_cust['lev1_group_freqs']) \
       .replace('lev2_group_freqs', tmp_tables_cust['lev2_group_freqs'])


    # In[ ]:




    # In[41]:

    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
    cursor = conn.cursor()


    cursor.execute(positive_objects_q_drop)
    cursor.execute(positive_objects_q_create)

    cursor.execute('select count(*) from positive_objects'.replace('positive_objects', tmp_tables_cust['positive_objects']))
    positive_objects_cnt = cursor.fetchone()[0]
    
    
    cursor.execute(logs_with_flags_q_drop)
    cursor.execute(logs_with_flags_q_create)



    cursor.execute(domain_group_freqs_q_drop)
    cursor.execute(domain_group_freqs_q_create)

    


    cursor.execute(lev0_group_freqs_q_drop)
    cursor.execute(lev0_group_freqs_q_create)


    cursor.execute(lev1_group_freqs_q_drop)
    cursor.execute(lev1_group_freqs_q_create)



    cursor.execute(lev2_group_freqs_q_drop)
    cursor.execute(lev2_group_freqs_q_create)



    cursor.execute(scores_q_drop)
    cursor.execute(scores_q_create)



    # In[ ]:

    if tinkoff_page == True:
	if unique_sync == False:
            positive_objects_t_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects_t']
	    positive_objects_t_q_create = 'create table ' + tmp_tables_cust['positive_objects_t'] + """ as
            select distinct
                concat(d.source_id, "-", a.ymd) as object_id
            from
                (select distinct 
                    h_uid_rk
                    ,ymd 
                from 
                    prod_rdds.r_visit
                where
                    ymd = "ymd_test"
                    and target_url_like_expression
                    and load_src = 'PRT.01'
                ) a
                inner join prod_dds.h_uid b on a.h_uid_rk = b.h_uid_rk and b.load_src = 'PRT.01'
                inner join prod_emart.dm_webuser_sync c on b.uid_str = c.source_id and c.source_type = 'tcs' and length(c.dmp_id) > 0
                inner join prod_emart.dm_webuser_sync d on c.dmp_id = d.dmp_id and d.source_type = 'liveinternet'  
            """.replace('ymd_test', ymd_test) \
               .replace('target_url_like_expression', target_url_like_expression)
        else:
            positive_objects_t_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects_t']
	    positive_objects_t_q_create = 'create table ' + tmp_tables_cust['positive_objects_t'] + """ as
            select
             object_id
            from
            (
             select
                concat(d.source_id, "-", a.ymd) as object_id
                ,d.source_id
                ,a.h_uid_rk
                ,count(*) over (partition by a.h_uid_rk) prt_rows_cnt
                ,count(*) over (partition by d.source_id) li_rows_cnt
             from
                (select distinct 
                 h_uid_rk
                 ,ymd 
                 from 
                 prod_rdds.r_visit
                 where 
                  ymd = "ymd_test"
                  and target_url_like_expression
                  and load_src = 'PRT.01'
                ) a
                inner join prod_dds.h_uid b on a.h_uid_rk = b.h_uid_rk and b.load_src = 'PRT.01'
                inner join prod_emart.dm_webuser_sync c on b.uid_str = c.source_id and c.source_type = 'tcs' and length(c.dmp_id) > 0
                inner join prod_emart.dm_webuser_sync d on c.dmp_id = d.dmp_id and d.source_type = 'liveinternet'
            ) t
            where 
             prt_rows_cnt = 1
             and li_rows_cnt = 1       
            """.replace('ymd_test', ymd_test) \
               .replace('target_url_like_expression', target_url_like_expression)

    else:
        positive_objects_t_q_drop = 'drop table if exists ' + tmp_tables_cust['positive_objects_t']
        positive_objects_t_q_create = 'create table ' + tmp_tables_cust['positive_objects_t'] + """ as
        select distinct 
            concat(id, "-", ymd) as object_id
        from
            prod_raw_liveinternet.access_log
        where
            ymd = "ymd_test"
            and url like "target_url_like_expression"
        """.replace('ymd_test', ymd_test) \
           .replace('target_url_like_expression', target_url_like_expression)



    # In[ ]:


    cursor.execute(positive_objects_t_q_drop)
    cursor.execute(positive_objects_t_q_create)

    cursor.execute('select count(*) from positive_objects_t'.replace('positive_objects_t', tmp_tables_cust['positive_objects_t']))
    positive_objects_t_cnt = cursor.fetchone()[0]

    # In[ ]:

    logs_with_flags_t_q_drop = 'drop table if exists ' + tmp_tables_cust['logs_with_flags_t']
    logs_with_flags_t_q_create = 'create table ' + tmp_tables_cust['logs_with_flags_t'] + """ as
    select
    concat(a.id, "-", a.ymd) as object_id
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)', 1) as domain
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?', 2) lev0
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?/?([^/]*)?', 3) lev1
    ,regexp_extract(regexp_extract(a.url, "([^\?]*)", 0), '^([^/]*)/?([^/]*)?/?([^/]*)?/?([^/]*)?', 4) lev2
    ,case when b.object_id is not null then 1 else 0 end as positive_flag
    from
     prod_raw_liveinternet.access_log a
     left join positive_objects_t b on concat(a.id, "-", a.ymd) = b.object_id
    where
     a.ymd  = "ymd_test"
    """.replace('ymd_test', ymd_test) \
       .replace('positive_objects_t', tmp_tables_cust['positive_objects_t'])



    cursor.execute(logs_with_flags_t_q_drop)
    cursor.execute(logs_with_flags_t_q_create)


    # In[ ]:

    up_logs_t_q_drop = 'drop table if exists ' + tmp_tables_cust['up_logs_t']
    up_logs_t_q_create = 'create table ' + tmp_tables_cust['up_logs_t'] + """ as
    select distinct 
     *
    from
    (
      select
       object_id
      ,positive_flag
      ,domain as up
      from
       logs_with_flags_t
      where
       length(domain) > 0
      union all
      select
       object_id
      ,positive_flag
      ,concat(domain,'[0]',lev0) as up
      from
       logs_with_flags_t
      where
       length(lev0) > 0
      union all
        select
       object_id
      ,positive_flag
      ,concat(domain,'[1]',lev1) as up
      from
       logs_with_flags_t
      where
       length(lev1) > 0
      union all
        select
       object_id
      ,positive_flag
      ,concat(domain,'[2]',lev2) as up
      from
       logs_with_flags_t
      where
       length(lev2) > 0
      ) t
    """.replace('logs_with_flags_t', tmp_tables_cust['logs_with_flags_t'])



    cursor.execute(up_logs_t_q_drop)
    cursor.execute(up_logs_t_q_create)


    # In[ ]:

    flags_x_scores_t_q_drop = 'drop table if exists ' + tmp_tables_cust['flags_x_scores_t']
    flags_x_scores_t_q_create = 'create table ' + tmp_tables_cust['flags_x_scores_t'] + """ as
    select
     a.object_id
     ,max(a.positive_flag) positive_flag
     ,max(coalesce(b.score, -10000)) as score
    from 
     up_logs_t a
     left join scores b on a.up = b.up
    where
     not (a.up rlike "domain_exclude")
     and not positive_flag is null
     and not score is null
    group by
     a.object_id
    """.replace('scores', tmp_tables_cust['scores']) \
       .replace('up_logs_t', tmp_tables_cust['up_logs_t']) \
       .replace('domain_exclude', '(^|\.)' + domain_exclude.split('.')[0] + '\.' + domain_exclude.split('.')[1])


    import os
    while os.path.isfile(flag_file) == True:
        time.sleep(30)
    f = open(flag_file, 'w+')
    f.close()


    cursor.execute(flags_x_scores_t_q_drop)
    cursor.execute(flags_x_scores_t_q_create)



    
    from pyspark import SparkContext, HiveContext
    sc = SparkContext()
    hc = HiveContext(sc)


    # In[ ]:

    hive_query = 'select positive_flag, score from flags_x_scores_t where score is not null and positive_flag is not null order by score desc' \
                 .replace('flags_x_scores_t', tmp_tables_cust['flags_x_scores_t'])
    data = hc.sql(hive_query)
    data_rdd_labeled_point = data.map(lambda x: (float(x.score), float(x.positive_flag)))

    from pyspark.mllib.evaluation import BinaryClassificationMetrics
    metrics = BinaryClassificationMetrics(data_rdd_labeled_point)

    AUC = metrics.areaUnderROC
    
    local_data_lift = data.map(lambda x: (int(x[0]), x[1])).collect()
    lift_df = lift(local_data_lift)
    spark_df_lift = hc.createDataFrame(lift_df)
    hc.registerDataFrameAsTable(spark_df_lift, "spark_df_lift")
    hc.sql('create table lift_t as select * from spark_df_lift'.replace('lift_t', tmp_tables_cust['lift']))
        
    sc.stop()
        
    elapsed_time = time.time() - start_time
    total_time = str(elapsed_time/60)
  
    
    results_q = """insert into hermes.lookalike_results values 
    (calc_id, "seg_name", AUC, "lift_table", "scores_table", positive_objects_train, positive_objects_test
    ,total_time, 'like_expression', "ymd_start", "ymd_end", "ymd_test")
    """.replace('calc_id', calc_id)\
       .replace('seg_name', seg_name)\
       .replace('AUC', str(AUC)) \
       .replace('lift_table', tmp_tables_cust['lift']) \
       .replace('scores_table', tmp_tables_cust['scores']) \
       .replace('positive_objects_train', str(positive_objects_cnt)) \
       .replace('positive_objects_test', str(positive_objects_t_cnt)) \
       .replace('total_time', str(total_time)) \
       .replace('like_expression', target_url_like_expression) \
       .replace('ymd_start', ymd_start) \
       .replace('ymd_end', ymd_end) \
       .replace('ymd_test', ymd_test)
    
    cursor.execute(results_q)

    cursor.execute(logs_with_flags_q_drop)
    cursor.execute(logs_with_flags_t_q_drop)
    cursor.execute(positive_objects_q_drop)
    cursor.execute(domain_group_freqs_q_drop)    
    cursor.execute(lev0_group_freqs_q_drop)
    cursor.execute(lev1_group_freqs_q_drop)
    cursor.execute(lev2_group_freqs_q_drop)
    cursor.execute(positive_objects_t_q_drop)
    cursor.execute(flags_x_scores_t_q_drop)

    cursor.close()

    os.remove(flag_file)