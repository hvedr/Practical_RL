
insert overwrite table user_kposminin.full_app_from_short_visits 
select
       a.phone_num,
       a.call_ymd,
       v.url_fragment,
       v.duration_sec, 
       v.visit_count, 
       v.average_visit_hour, 
       v.load_src, 
       v.ymd as visit_ymd
  from user_kposminin.full_app_from_short_id a
 inner join prod_odd.visit_feature v on v.id = a.id
 where v.ymd = '2017-05-31'
   and a.call_ymd between date_add('2017-05-31',1) and date_add('2017-05-31',180)