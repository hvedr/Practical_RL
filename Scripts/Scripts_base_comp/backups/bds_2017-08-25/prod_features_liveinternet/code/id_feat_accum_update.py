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
        
    ymd_to_calc   = datetime.datetime.combine(datetime.date.today(),datetime.time(0,0,0)) - datetime.timedelta(days = 1) # yesterday with 0 time 
    month_change = (ymd_to_calc.day == 1)
    acc_last_ymd  = ymd_to_calc - datetime.timedelta(days = 1) 
    acc_first_ymd = add_months(datetime.datetime(ymd_to_calc.year,ymd_to_calc.month,1), -acc_months)
    cur_month_first_ymd = datetime.datetime(ymd_to_calc.year,ymd_to_calc.month,1)
    
    ymd_loaded     = get_last_visits_date(cursor)
    print('{}. Getting visits later than {}.\n'.format(datetime.datetime.now(),ymd_loaded))
    sys.stdout.flush()
    
    cnt = 0
    while (ymd_to_calc.strftime('%Y-%m-%d') > ymd_loaded):
        cnt += 1
        if cnt > 70:
            print('{}. Failed to wait for visits to be loaded later than {}. Terminating.\n'.format(datetime.datetime.now(),ymd_loaded) + '*'*60)
            sys.stdout.flush()
            exit(1)
        print('Sleeping for 1000 sec. ymd_to_calc: {}. ymd_loaded: {}.'.format(ymd_to_calc.strftime('%Y-%m-%d'),ymd_loaded))
        sys.stdout.flush()
        time.sleep(1000)
        ymd_loaded = get_last_visits_date(cursor)
    
    print('{} Got visits for {}. Calculating signals for {}'.format(datetime.datetime.now(),ymd_loaded,ymd_to_calc.strftime('%Y-%m-%d')))
    sys.stdout.flush()

    c = calc_cred_score()

    # calc 1 day
    c.calc_days([ymd_to_calc], merge = False, clean = True)

    if(month_change):
        acc_to_del_first_ymd = add_months(acc_first_ymd, -1)
        acc_to_del_last_ymd  = acc_last_ymd
        first_days = [add_months(acc_first_ymd,i) for i in range(acc_months + 1)]
        acc_to_merge = [(fd,nfd - datetime.timedelta(days = 1)) for fd,nfd in zip(first_days[:-1],first_days[1:])]
        c.drop_partitions(((acc_to_del_first_ymd,acc_to_del_last_ymd),))
        c.merge_days(acc_to_merge,clean = False)

    # add to current accumulator
    c.merge_days([(acc_first_ymd,acc_last_ymd),(ymd_to_calc,ymd_to_calc)])
    
    # add to current month accumulator
    if(not month_change):
        c.merge_days([(cur_month_first_ymd,acc_last_ymd),(ymd_to_calc,ymd_to_calc)])
    
    # calc phone features table
    c.calc_full_feat(acc_first_ymd,ymd_to_calc)

    # clean
    partitions_to_clean = [
       (ymd_to_calc + datetime.timedelta(days = -7), ymd_to_calc + datetime.timedelta(days = -7)),
       (acc_first_ymd, acc_last_ymd),
       (cur_month_first_ymd, acc_last_ymd)
    ]

    c.drop_partitions(partitions_to_clean)

    c.clean_full_feat_partitions(((acc_first_ymd, ymd_to_calc + datetime.timedelta(days = -7)),))
    
    #print(c.get_query())
    c.execute_query(cursor = cursor)
    
    print('{}: id_feat_accum update for {} finished. Created partitions:({},{}),({},{}),({},{}).').format(
        datetime.datetime.now(),
        ymd_to_calc.strftime('%Y-%m-%d'),
        ymd_to_calc.strftime('%Y-%m-%d'),
        ymd_to_calc.strftime('%Y-%m-%d'),
        acc_first_ymd.strftime('%Y-%m-%d'),
        ymd_to_calc.strftime('%Y-%m-%d'),
        cur_month_first_ymd.strftime('%Y-%m-%d'),
        ymd_to_calc.strftime('%Y-%m-%d')
    )


main()