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

test_query="select
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
 ((t.id is Null) or (t.ymd between date_add(a.ymd,1) and date_add(a.ymd,3)))
"




for d in '2016-10-11'
do
   query=`echo "$test_query" | sed "s/@date/$d/g" | sed "s/\n/ /g"`
   i=`echo "$d" | sed "s/-//g"`
   rm -f /home/k.p.osminin/external_hdfs/ha_raw_$i.txt /home/k.p.osminin/external_hdfs/ha_proc_$i.txt
   hive -e "$query" > /home/k.p.osminin/external_hdfs/ha_raw_$i.txt
   python /home/k.p.osminin/scripts/hasher1.py /home/k.p.osminin/external_hdfs/ha_raw_$i.txt /home/k.p.osminin/external_hdfs/ha_proc_$i.txt

done

