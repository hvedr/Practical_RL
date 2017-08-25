select
  concat(if(t.id is Null,0,1),' ',concat_ws(' ',features_set))
from
  (select 
    v.id
   ,collect_set(concat(regexp_replace(v.urlfr,"[': \"]",""),':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
  where 
    v.ymd = '2016-10-12'
  group by v.id
  ) a
  left join (select id from prod_features_liveinternet.user_action 
  where ymd = '2016-10-13'
    and action_type = 'tinkoff_platinum_complete_application'
  ) t on t.id = a.id
; 

