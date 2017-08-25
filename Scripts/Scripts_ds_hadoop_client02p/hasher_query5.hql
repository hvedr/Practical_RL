select
  concat(if(t.id is Null,0,1),' ',concat_ws(' ',features_set))
from
  (select 
    v.id
   ,v.ymd
   ,collect_set(concat(regexp_replace(v.urlfr,"[': \"]",""),':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
  where 
    v.ymd = '2016-10-01'
  group by v.id
  ) a
  left join (select id,ymd from prod_features_liveinternet.user_action 
  where action_type = 'tinkoff_platinum_complete_application'
  ) t on t.id = a.id and t.ymd = date_add(a.ymd,1)
where
 (not t.id is Null) or RAND() < 0.01
; 

