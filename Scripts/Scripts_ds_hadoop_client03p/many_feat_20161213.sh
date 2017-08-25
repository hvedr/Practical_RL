#!/bin/bash
rm /data1/share/kosm/data/la_many_feat_20161108.txt

hive -f la_many_feat_20161108.hql > /home/k.p.osminin/data/la_many_feat_20161108.txt

/opt/share/LightGBM-master/lightgbm config=test.conf data=home/k.p.osminin/data/la_many_feat_20161108.txt  output_result=home/k.p.osminin/data/la_many_feat_predict_20161108.txt > log_ccall_many_feat_20161223.txt
perl -ne 'print substr($_,0,1)' /home/k.p.osminin/data/la_many_feat_20161108.txt > /home/k.p.osminin/data/la_many_feat_labels_20161108.txt
