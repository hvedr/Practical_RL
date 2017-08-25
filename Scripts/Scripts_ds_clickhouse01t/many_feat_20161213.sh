#!/bin/bash
rm /data1/share/kosm/data/la_many_feat_20161108.txt

beeline -u "jdbc:hive2://ds-hadoop-cs01p:10000/" -n kposminin --incremental=true --showheader=false --outputformat=tsv2 --maxwidth=5000 --silent=true --showWarnings=false --showNestedErrs=false --verbose=false --nullemptystring=true -f /home/k.osminin/scripts/la_many_feat_20161108.hql > /data1/share/kosm/data/la_many_feat_20161108.txt
cat /data1/share/kosm/data/la_many_feat_20161108.txt | tail -n +8 | head -n -3 > /data1/share/kosm/data/la_many_feat_20161108_1.txt

/data1/share/LightGBM/lightgbm  config=/home/k.osminin/lgbm_files/test.conf data=/data1/share/kosm/data/la_many_feat_20161108_1.txt  output_result=/data1/share/kosm/data/la_many_feat_predict_20161108_1.txt> /home/k.osminin/lgbm_files/log_ccall_many_feat_20161222.txt
perl -ne 'print substr($_,0,1)' /data1/share/kosm/data/la_many_feat_20161108_1.txt > /data1/share/kosm/data/la_many_feat_label_20161108_1.txt
