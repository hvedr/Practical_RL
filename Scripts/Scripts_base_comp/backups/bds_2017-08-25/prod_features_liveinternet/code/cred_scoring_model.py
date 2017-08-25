
# coding: utf-8

import cPickle
import re, sys, os
import numpy as np
import pandas as pd
import datetime, time
import cPickle
from pylightgbm.models import GBMClassifier
 
#from sklearn.feature_extraction import DictVectorizer
#import sklearn, sklearn.cross_validation
#import hashlib
#from collections import Counter

def add_months(ymd,months):
    '''ymd is datetime.datetime.'''
    year = ymd.year + (ymd.month + months - 1) / 12
    month = (ymd.month + months - 1) % 12 + 1
    day = ymd.day
    return datetime.datetime(year,month,day)


def check_features_dates(hc, first_calc_ymd, last_calc_ymd):
    return ( len(hc.sql('''select 1 from prod_features_liveinternet.phone_x_feature 
                           where first_calc_ymd = "#first_calc_ymd" and last_calc_ymd = "#last_calc_ymd"
                           limit 1'''
                               .replace( "#first_calc_ymd",first_calc_ymd.strftime('%Y-%m-%d'))
                               .replace( "#last_calc_ymd",last_calc_ymd.strftime('%Y-%m-%d'))
                        ).collect()
                 ) > 0
            )

def main():

    print('{0}: cred scoring started.').format(datetime.datetime.now())
    sys.stdout.flush()
    
    hadoop_dir = '/prod_features/liveinternet/phone_cred_scor_tmp'
    local_dir = '/home/bigdatasys/projects/prod_features_liveinternet/data'

    execfile('/home/bigdatasys/.ipython/profile_spark/startup/00-pyspark-setup.py')

    from pyspark import SparkConf, SparkContext, HiveContext
    #import org.apache.log4j.Logger
    #import org.apache.log4j.Level
    #Logger.getLogger("org").setLevel(Level.OFF)
    #Logger.getLogger("akka").setLevel(Level.OFF)
    
    hive_config_query = '''
    set hive.vectorized.execution.enabled=true;
    set hive.vectorized.execution.reduce.enabled = true;
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
    set mapreduce.map.failures.maxpercent=5;
    '''
    try:
        sc.stop()
    except:
        pass

    conf = (SparkConf()
            .set("spark.executor.instances", 16)
            .set("spark.driver.maxResultSize", "8g")
            .set('spark.driver.memory','8g')
            .set("spark.executor.memory", '8g')
            .set("spark.yarn.executor.memoryOverhead", 1048)
           )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    hc = HiveContext(sc)
    
    for q in hive_config_query.split(';'):
        try:
            hc.sql(q)
        except:
            pass
    
    acc_months = 2
    # yesterday with 0 time
    ymd_to_calc   = datetime.datetime.combine(datetime.date.today(),datetime.time(0,0,0)) - datetime.timedelta(days = 1)  
    first_calc_ymd = add_months(datetime.datetime(ymd_to_calc.year,ymd_to_calc.month,1), - acc_months)
    
    print('{}. Getting features for ({},{}).'
                    .format(datetime.datetime.now(),first_calc_ymd.strftime('%Y-%m-%d'), ymd_to_calc.strftime('%Y-%m-%d')))
    sys.stdout.flush()
    
    cnt = 0
    while (not check_features_dates(hc, first_calc_ymd, ymd_to_calc)):
        cnt += 1
        if cnt > 70:
            print('{}. Failed to get features to be loaded for ({},{}). Terminating.\n'
                    .format(datetime.datetime.now(),first_calc_ymd.strftime('%Y-%m-%d'), ymd_to_calc.strftime('%Y-%m-%d')) + '*'*60)
            sys.stdout.flush()
            exit(1)
        print('{}.Sleeping for 1000 sec. (first_calc_ymd,ymd_to_calc): ({},{}).'
              .format(datetime.datetime.now(),first_calc_ymd.strftime('%Y-%m-%d'), ymd_to_calc.strftime('%Y-%m-%d')))
        sys.stdout.flush()
        time.sleep(1000)
    
    print('{}. Got features or ({},{}). Calculating cred scores.'
                    .format(datetime.datetime.now(),first_calc_ymd.strftime('%Y-%m-%d'), ymd_to_calc.strftime('%Y-%m-%d')))
    sys.stdout.flush()
    
    hand_pipe1 = cPickle.load(open('/home/bigdatasys/projects/prod_features_liveinternet/model/cred_scor_classifier_pipe3.pck','r'))
    feats                = hand_pipe1[0][1]
    categorical_features = hand_pipe1[1][1]
    labelencoder         = hand_pipe1[2][1]
    target_encoded       = hand_pipe1[3][1]
    clf                  = hand_pipe1[4][1]
    
    hc.sql('drop table if exists prod_features_liveinternet.phone_cred_scor_tmp')
    os.popen('hadoop fs -mkdir -p {hadoop_dir}'.format(hadoop_dir=hadoop_dir)).read()
    os.popen('hadoop fs -rm {hadoop_dir}/*'.format(hadoop_dir=hadoop_dir)).read()
    os.popen('rm {local_dir}/*'.format(local_dir=local_dir)).read()

    
    for ind in list('0123456789abcdef'):
        df_batch = (hc.sql('''select * from prod_features_liveinternet.phone_x_feature 
                        where  first_calc_ymd = "#first_calc_ymd" 
                               and last_calc_ymd = "#last_calc_ymd"
                               and substr(md5(phone_num),1,1) = '#ind' '''
                       .replace( "#first_calc_ymd",first_calc_ymd.strftime('%Y-%m-%d'))
                       .replace( "#last_calc_ymd", ymd_to_calc.strftime('%Y-%m-%d'))
                       .replace( "#ind",ind)
                   )        
                .toPandas()
                 )
        for v in categorical_features:
            df_batch.loc[:,v]  = df_batch.loc[:,v].map(lambda f: labelencoder[v].get(f,-1))
            df_batch.loc[:,v + '_encoded'] = df_batch[v].map(target_encoded[v])
        df_batch['pred'] = clf.predict_proba(df_batch[feats])[:,1]
        df_batch[['phone_num','pred','last_calc_ymd']].to_csv('{local_dir}/phone_cred_scor_tmp_{ind}.txt'.format(ind=ind,local_dir=local_dir,hadoop_dir=hadoop_dir),index = False,header = False) #,quote_char = ''
        os.popen('hadoop fs -put {local_dir}/phone_cred_scor_tmp_{ind}.txt {hadoop_dir}/phone_cred_scor_tmp_{ind}.txt'.format(ind=ind,local_dir=local_dir,hadoop_dir=hadoop_dir)).read()
        print('{}. {} handled.'.format(datetime.datetime.now(),ind))
        sys.stdout.flush()

    
    end_query = '''
    create external table if not exists prod_features_liveinternet.phone_cred_scor_tmp
    (
      `phone_num` string,
      `score` float,  
      `ymd` string
      )
    ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
        LOCATION
          '{hadoop_dir}'
    ;

    insert overwrite table prod_lookalike.phone_x_segment partition (segment_nm,ymd)
    select 
      phone_num,
      score,
      current_timestamp() as load_dttm,
      'cred_score_1' as segment_nm,
      ymd
    from prod_features_liveinternet.phone_cred_scor_tmp
    ;    
    '''.format(hadoop_dir=hadoop_dir)
    
    for q in end_query.split(';'):
        if re.search('[^ \t\n]',q):
            hc.sql(q)


main()
print('*'*100 + '{}. Task ended successfully'.format(datetime.datetime.now()))