#!/bin/bash

query1="select concat(label,' ',features_set) as str from user_kposminin.la_max_score_and_hash_@ind"



for d in '2016-10-13' '2016-10-20'
do
   i=`echo "$d" | sed "s/-//g"`
   query=`echo "$query1" | sed "s/@ind/$i/g" | sed "s/\n/ /g"`
   rm -f /home/k.p.osminin/external_hdfs/ha_raw_lah_$i.txt /home/k.p.osminin/external_hdfs/ha_proc_lah_$i.txt
   hive -e "$query" > /home/k.p.osminin/external_hdfs/ha_raw_lah_$i.txt
   python /home/k.p.osminin/scripts/hasher2.py /home/k.p.osminin/external_hdfs/ha_raw_lah_$i.txt /home/k.p.osminin/external_hdfs/ha_proc_lah_$i.txt
   /home/k.p.osminin/xgboost/xgboost /home/k.p.osminin/xgboost_test/hasher_example1.conf task=pred model_in=0010_iterlearn.model test:data=/home/k.p.osminin/external_hdfs/ha_proc_lah_$i.txt
   mv /home/k.p.osminin/xgboost_test/pred.txt /home/k.p.osminin/external_hdfs/pred_$i.txt

done

