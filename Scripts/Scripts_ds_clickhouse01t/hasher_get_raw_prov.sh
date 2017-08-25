#!/bin/bash

train_query="select
  concat(if(t.id is Null,0,1),' ',concat_ws(' ',features_set))
from
  (select 
    v.id
   ,v.ymd
   ,collect_set(concat(regexp_replace(v.urlfr,\"[': \\\"]\",\"\"),':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
  where 
    v.ymd = '@date'
  group by v.id,v.ymd
  ) a
  left join (select id,ymd from prod_features_liveinternet.user_action 
  where action_type = 'tinkoff_platinum_complete_application'
  ) t on t.id = a.id 
where
 ((not t.id is Null) and ( t.ymd between date_add(a.ymd,1) and date_add(a.ymd,3))) or RAND() < 0.01
"

test_query1="select
  concat(if(t.id is Null,0,1),' ',concat_ws(' ',features_set))
from
  (select 
    v.id
   ,v.ymd
   ,collect_set(concat(regexp_replace(v.urlfr,\"[': \\\"]\",\"\"),':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
  where 
    v.ymd = '@date'
  group by v.id,v.ymd
  ) a
  left join (select id,ymd from prod_features_liveinternet.user_action 
  where action_type = 'tinkoff_platinum_complete_application'
  ) t on t.id = a.id 
where
 ((t.id is Null) or ( t.ymd between date_add(a.ymd,1) and date_add(a.ymd,3)))
 and a.id < 'n'
"
test_query2="select
  concat(if(t.id is Null,0,1),' ',concat_ws(' ',features_set))
from
  (select 
    v.id
   ,v.ymd
   ,collect_set(concat(regexp_replace(v.urlfr,\"[': \\\"]\",\"\"),':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
  where 
    v.ymd = '@date'
  group by v.id,v.ymd
  ) a
  left join (select id,ymd from prod_features_liveinternet.user_action 
  where action_type = 'tinkoff_platinum_complete_application'
  ) t on t.id = a.id 
where
 ((t.id is Null) or ( t.ymd between date_add(a.ymd,1) and date_add(a.ymd,3)))
 and a.id >= 'n'
"


for d in '2016-10-11'
do
   query1=`echo "$test_query1" | sed "s/@date/$d/g" | sed "s/\n/ /g"`
   query2=`echo "$test_query2" | sed "s/@date/$d/g" | sed "s/\n/ /g"`
   i=`echo "$d" | sed "s/-//g"`
   rm -f /data1/share/kosm/data/ha_raw_$i.txt /data1/share/kosm/data/ha_proc_$i.txt
   beeline -u "jdbc:hive2://ds-hadoop-cs01p:10000/" -n kposminin -e "$query1" >> /data1/share/kosm/data/ha_raw_$i.txt
   beeline -u "jdbc:hive2://ds-hadoop-cs01p:10000/" -n kposminin -e "$query2" >> /data1/share/kosm/data/ha_raw_$i.txt
   python /home/k.osminin/scripts/hasher1.py /data1/share/kosm/data/ha_raw_$i.txt /data1/share/kosm/data/ha_proc_$i.txt

done

