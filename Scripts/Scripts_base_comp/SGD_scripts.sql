create table user_kposminin.urlfr_dict as 
with src as (select * from user_kposminin.visits_ext where ymd = '2016-04-12')

select 
  urlfr, 
  rank() over (order by urlfr) as urlfr_num,
  count(*) as cnt 
from src 
group by urlfr 
having cnt > 500
;


create table user_kposminin.id_dict as 
with src as (select * from user_kposminin.visits_ext where ymd = '2016-04-12')
select 
  id,
  rank() over (order by id) as id_num,
  count(*) as cnt 
from src 
group by urlfr 
having cnt > 10 and cnt < 500
;


-- 

create table user_kposminin.visits_enum_dense_20160412 as
with src as (select * from user_kposminin.visits_ext where ymd = '2016-04-12')
select
  s.id, i.id_num,
  s.urlfr, u.urlfr_num,
  s.cnt
from
  src s
  inner join user_kposminin.urlfr_dict u on s.urlfr = u.urlfr
  inner join user_kposminin.id_dict i on s.id = i.id
where 
u.cnt > 1000
and i.cnt > 30;

select count(*) as tot_cnt,count(distinct id_num) as id_cnt, count(distinct urlfr_num) as uf_cnt from visits_enum_dense_20160412
tot_cnt	id_cnt	uf_cnt
-- 1'161'260'847	32'849'395	203'201