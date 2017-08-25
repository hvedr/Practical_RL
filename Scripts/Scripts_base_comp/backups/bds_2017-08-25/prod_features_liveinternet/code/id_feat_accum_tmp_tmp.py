# Updated 2017-06-29 by k.p.osminin

# coding: utf-8

import datetime,os,re, sys, time
from id_feat_class import calc_cred_score
from pyhive import hive
    

def add_months(ymd,months):
    '''ymd is datetime.datetime.'''
    year = ymd.year + (ymd.month + months - 1) / 12
    month = (ymd.month + months - 1) % 12 + 1
    day = ymd.day
    return datetime.datetime(year,month,day)

def get_last_visits_date(cursor):
    cursor.execute('select max(ymd) from prod_odd.visit_feature')
    return cursor.fetchone()[0]

def get_last_model_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-02-12") from prod_lookalike.phone_x_segment where segment_nm = "#segment_nm"'.replace('#segment_nm', segment_nm))
    return cursor.fetchone()[0]

def get_last_main_seg_calc_date(cursor):
    cursor.execute('select coalesce(max(ymd),"2017-02-12") from prod_lookalike.phone_x_segment where segment_nm = "#main_seg"'.replace('#main_seg', main_seg))
    return cursor.fetchone()[0]



def main():
    
    acc_months = 2
    
    print('{0}: id_feat_accum update started.').format(datetime.datetime.now())
    
    #init connection
    
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
    
    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
    cursor = conn.cursor()
    
    c = calc_cred_score()
    
    acc_to_merge = [
        ('2017-08-01','2017-08-14'),
         ('2017-08-16','2017-08-16'),
         ('2017-08-17','2017-08-17'),
         ('2017-08-18','2017-08-18'),
         ('2017-08-20','2017-08-20')
    ]
    
    def conv(s):
        return datetime.datetime.strptime(s,'%Y-%m-%d').date()
    
    acc_to_merge_d = [(conv(e[0]),conv(e[1])) for e in acc_to_merge]
    #c.merge_days(acc_to_merge_d, clean = False)
    c.calc_full_feat(conv('2017-06-01'),conv('2017-08-20'))
    print(c.get_query())
    c.execute_query(cursor = cursor)

    print('success')
 

main()