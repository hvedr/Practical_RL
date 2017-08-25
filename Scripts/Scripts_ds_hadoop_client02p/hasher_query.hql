-- select '1 vk.com:4 ok.ru#thisis:4 lib.ru#arr:7' as s
-- union all select '0 ok.ru:1 google.ru#th:8 lib.ru#arr:6' as s;

select
  concat(label,' ',concat_ws(' ',features_set))
from
  (select 
    max(if(t.id is Null,0,1)) as label
   ,collect_set(concat(v.urlfr,':',cnt)) as features_set
  from
    prod_features_liveinternet.visits v
    left join prod_features_liveinternet.user_action t on t.id = v.id
  where 
    v.ymd = '2016-10-12'
    and t.ymd = '2016-10-13'
    and t.action_type = 'tinkoff_platinum_complete_application'
  group by v.id) a;
