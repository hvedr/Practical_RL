insert overwrite 
 table prod_churn.account_to_score partition(ymd)
select
 a.account_rk
 ,max(current_timestamp)
 ,max("YMD_TO_PROCESS") as ymd
from
 (select
   account_rk
   ,card_rk  
  from
   prod_dds.bank_card_chain
  where
   "YMD_TO_PROCESS" between valid_from_dttm and valid_to_dttm  
  ) a 
 left semi join (
   select
    card_rk
   from
    prod_dds.bank_card_plastic
   where
    card_design_id in ('2','6','8','21','25','26','29','32','47')
   ) b on a.card_rk = b.card_rk
  left semi join
   (
     select
      account_rk
     from
      prod_emart.credit_card_account
     where
      credit_status_cd = 'NRM'
      and account_status_cd = 'ACT'
	  and utilization_dt is not null
     ) c on a.account_rk = c.account_rk
group by
 a.account_rk
;


insert overwrite
 table prod_churn.features_credit_card_account partition(ymd)
select
 a.account_rk
 ,max(c.tarif_id) as tarif_id
 ,max(case when a.ymd between b.valid_from_dttm and b.valid_to_dttm then b.LIMIT_CHANGE_CD end) as limit_change_cd
 ,max(c.mail_stmt_send_flg) as mail_stmt_send_flg
 ,max(c.email_stmt_send_flg) as email_stmt_send_flg
 ,max(c.current_limit_amt) as current_limit_amt
 ,max(c.initial_limit_amt) as initial_limit_amt
 ,max(c.current_limit_amt/c.initial_limit_amt) as current_to_initial_limit
 ,max(case when a.ymd between b.valid_from_dttm and b.valid_to_dttm then b.CREDIT_PAYMENT_PROTECTION_CD end) as credit_payment_protection_cd
 ,max(case when
     to_date(b.valid_from_dttm) between a.ymd and date_add(a.ymd, -30)
     or to_date(b.valid_to_dttm) between a.ymd and date_add(a.ymd, -30)     
     or a.ymd between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
     or date_add(a.ymd, -30) between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
      then b.CURRENT_LIMIT_AMT end) as max_LIMIT_AMT_1m
 ,min(case when
     to_date(b.valid_from_dttm) between a.ymd and date_add(a.ymd, -30)
     or to_date(b.valid_to_dttm) between a.ymd and date_add(a.ymd, -30)     
     or a.ymd between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
     or date_add(a.ymd, -30) between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm) then b.CURRENT_LIMIT_AMT end) as min_limit_amt_1m
 ,(max(case when
     to_date(b.valid_from_dttm) between a.ymd and date_add(a.ymd, -30)
     or to_date(b.valid_to_dttm) between a.ymd and date_add(a.ymd, -30)
     or a.ymd between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
     or date_add(a.ymd, -30) between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
      then b.CURRENT_LIMIT_AMT end))
      /
      (min(case when
     to_date(b.valid_from_dttm) between a.ymd and date_add(a.ymd, -30)
     or to_date(b.valid_to_dttm) between a.ymd and date_add(a.ymd, -30)     
     or a.ymd between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
     or date_add(a.ymd, -30) between to_date(b.valid_from_dttm) and to_date(b.valid_to_dttm)
      then b.CURRENT_LIMIT_AMT end)) as max_to_min_limit_1m
 ,max(case when b.valid_to_dttm between date_add(a.ymd, -30) and from_unixtime(unix_timestamp(a.ymd, 'yyyy-MM-dd')) and b.credit_status_cd in ('CRL') then 1 else 0 end) CRL_1m
 ,max(case when b.valid_to_dttm between date_add(a.ymd, -30) and from_unixtime(unix_timestamp(a.ymd, 'yyyy-MM-dd')) and b.credit_status_cd in ('DNL') then 1 else 0 end) DNL_1m
 
 ,max((unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(c.open_dt, 'yyyy-MM-dd'))/60/60/24) as days_open
 ,max((unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(c.utilization_dt, 'yyyy-MM-dd'))/60/60/24) as days_utilized
  ,max(c.balance_amt) balance_amt
 
 ,max(c.balance_amt/c.current_limit_amt) as limit_utilization
 ,max(current_timestamp)
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a
 left join prod_dds.credit_card_account b on a.account_rk = b.account_rk
 left join prod_emart.credit_card_account c on a.account_rk = c.account_rk
where
 a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;


insert overwrite
 table prod_churn.features_account partition(ymd)
select
 a.account_rk
 ,count(distinct case when h.close_dt is null and h.core_account_type_cd = 'COL' then h.account_rk end) as col_cnt
 ,count(distinct case when h.close_dt between date_add(a.ymd, -30) and a.ymd then h.account_rk end) as core_recently_closed
 ,count(distinct case when h.close_dt is null and h.core_account_type_cd = 'CUR' then h.account_rk end) as cur_cnt
 ,count(distinct case when h.close_dt is null and h.core_account_type_cd = 'DEP' then h.account_rk end) as deposit_cnt
 ,count(distinct case when h.close_dt is null and h.core_account_type_cd = 'LEG' then h.account_rk end) as leg_cnt
 ,count(distinct case when i.close_dt is null and i.loan_account_type_cd in ('VKR', 'CLN') then i.account_rk end) as loan_cnt
 ,count(distinct g.account_rk) - 1 as other_accounts_of_customer
 ,count(distinct case when (h.close_dt is null and h.account_rk is not null) 
        or (i.close_dt is null and i.account_rk is not null) then g.account_rk end) as other_active_accounts_of_customer  
 ,count(distinct case when h.close_dt is null and h.core_account_type_cd = 'SAV' then h.account_rk end) as sav_cnt
 ,max(current_timestamp)
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a
 left join prod_dds.customer_x_financial_account f on a.account_rk = f.account_rk 
 left join prod_dds.customer_x_financial_account g on f.customer_rk = g.customer_rk and f.relationship_to_account_cd = 'PRM' 
 left join prod_emart.core_account h on h.account_rk = g.account_rk 
 left join prod_emart.loan_account i on i.account_rk = g.account_rk 
where
 a.ymd = "YMD_TO_PROCESS"
 and
 (
 (a.ymd between from_unixtime(unix_timestamp(f.valid_from_dttm)) and from_unixtime(unix_timestamp(f.valid_to_dttm)) or f.valid_to_dttm is null) 
 and (a.ymd between from_unixtime(unix_timestamp(g.valid_from_dttm)) and from_unixtime(unix_timestamp(g.valid_to_dttm)) or g.valid_to_dttm is null)
   )
group by
  a.account_rk
;


insert overwrite
 table prod_churn.features_trans partition(ymd)
select
 a.account_rk 
 ,count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'C' then trans.transaction_rk end) credit_trans_cnt_1m
 ,count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and a.ymd then trans.currency_cd end) as currencies_2m
 ,min(case 
       when (unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(trans.real_transaction_dttm ))/60/60/24 between 0 and 60 
       and trans.credit_debit_flg = 'C'
      then (unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(trans.real_transaction_dttm ))/60/60/24
      else 60
      end) days_since_last_credit_trans 
  ,min(case 
       when (unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(trans.real_transaction_dttm ))/60/60/24 between 0 and 60 
       and trans.credit_debit_flg = 'D'
      then (unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(trans.real_transaction_dttm ))/60/60/24
      else 60
      end )
      days_since_last_debit_trans
  ,count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'D' then trans.transaction_rk end) as debit_trans_cnt_1m 
  , (coalesce(count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd then trans.transaction_rk end),0) + 0.5)
  /
  (coalesce(count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then trans.transaction_rk end),0) + 0.5)
   as trans_1m_to_trans_prev_m
  ,count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'C' then trans.transaction_rk end) as trans_cnt_1m
  ,count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then trans.transaction_rk end) as trans_cnt_prev_m 
  ,(coalesce(count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'C' then trans.transaction_rk end),0) + 0.5)
  /
  (coalesce(count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and credit_debit_flg = 'C' then trans.transaction_rk end),0) + 0.5)
   as trans_credit_1m_to_trans_prev_m
  ,coalesce(sum(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'C' then trans.transaction_amt_rur end),0) as trans_credit_sum_1m
  ,(coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and trans.credit_debit_flg = 'C' then trans.transaction_amt_rur end ),0)+ 30)
  /
  (coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and trans.credit_debit_flg = 'C' then trans.transaction_amt_rur end),0)+ 30)
  as trans_credit_sum_1m_to_trans_sum_prev_m
  ,coalesce(sum(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and trans.credit_debit_flg = 'C' then trans.transaction_amt_rur end),0) as trans_credit_sum_prev_m
  ,(coalesce(count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'D' then trans.transaction_rk end),0) + 0.5)
   /
   (coalesce(count(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and credit_debit_flg = 'D' then trans.transaction_rk end),0) + 0.5)
   as trans_debit_1m_to_trans_prev_m
  ,sum(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'D' then trans.transaction_amt_rur end) as trans_debit_sum_1m
  ,(coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and credit_debit_flg = 'D' then trans.transaction_amt_rur end),0)+ 30)
   /
   (coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and trans.credit_debit_flg = 'D' then trans.transaction_amt_rur end),0)+ 30)
   as trans_debit_sum_1m_to_trans_sum_prev_m
  ,sum(case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and trans.credit_debit_flg = 'D' then trans.transaction_amt_rur end) as trans_debit_sum_prev_m
  ,count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd and trans.transaction_type_cd = 'GCS' then trans.transaction_rk end) as trans_gcs_cnt_1m
  ,(coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd then trans.transaction_amt_rur end ),0)+ 30)
   /
   (coalesce(sum(  case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then trans.transaction_amt_rur end),0)+ 30)
   as trans_sum_1m_to_trans_sum_prev_m
  ,count(distinct case when to_date(trans.real_transaction_dttm) between date_add(a.ymd, -30) and a.ymd then trans.transaction_type_cd end) as trans_types_1m 
  ,max(current_timestamp)
 
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a
 left join (select distinct
             account_rk
             ,transaction_rk
             ,transaction_type_cd
             ,credit_debit_flg
             ,real_transaction_dttm
             ,transaction_amt_rur
             ,currency_cd 
            from
             prod_emart.financial_transaction  
            where 
             transaction_status_cd = 'PST' 
             and financial_account_type_cd = 'CCR'
             and to_date(real_transaction_dttm) >= date_add("YMD_TO_PROCESS", -60)) trans on a.account_rk = trans.account_rk
where
 a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;

insert overwrite
 table prod_churn.features_cust partition(ymd)
select
 a.account_rk
 ,max((unix_timestamp("YMD_TO_PROCESS", 'yyyy-MM-dd') - unix_timestamp(cust.BIRTH_DT))/60/60/24/365) age
 ,max(cust.auth_type) auth_type
 ,max(faap.car_own_flg) car_own_flg
 ,max(faap.car_type_flg) car_type_flg
 ,max(cust.children_cnt) children_cnt
 ,max(cust.cust_value_cd) cust_value_cd
 ,max(em_cust.education_level_cd) education_level_cd
 ,max(cust.email_valid_flg) email_valid_flg
 ,max(cust.gender_cd) gender_cd
 ,max(cust.has_email) has_email
 ,max(cust.ib_status_cd) ib_status_cd
 ,max((unix_timestamp("YMD_TO_PROCESS", 'yyyy-MM-dd') - unix_timestamp(cust.ibank_register_first_dt))/60/60/24) as ibank_register_days
 ,max(cust.ibank_register_flg) ibank_register_flg
 ,max(cust.id_block_flg) id_block_flg
 ,max(em_cust.insider_flg) insider_flg
 ,max(cust.job_org_own_bsns_flg) job_org_own_bsns_flg
 ,max(cust.marital_status_cd) marital_status_cd
 ,max(cust.monthly_income_amt) monthly_income_amt
 ,max(cust.pensioner_flg) pensioner_flg
 ,max(case when (
   cust.suppress_mail_flg = 'Y' or
   cust.suppress_email_flg = 'Y' or
   cust.suppress_sms_flg = 'Y' or
   cust.suppress_call_flg = 'Y') then 'Y' else 'N' end) as suppress_flg
 ,max(cust.suppress_limit) suppress_limit
 ,max(current_timestamp)
 
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a 
 left join 
  (select 
    open_account_rk
    ,row_number() over (partition by open_account_rk order by application_dt) r_n
    ,car_type_flg    
    ,car_own_flg 
   from
    prod_emart.financial_account_application
   ) faap on faap.open_account_rk = a.account_rk and faap.r_n = 1
 left join 
  (select 
    customer_rk
	,account_rk
   from 
	prod_dds.customer_x_financial_account f
   where 
	"YMD_TO_PROCESS" between to_date(valid_from_dttm) and to_date(valid_to_dttm)
	and relationship_to_account_cd = 'PRM'
    ) f on a.account_rk = f.account_rk 
 left join 
  (select
    customer_rk
    ,auth_type
    ,birth_dt
    ,children_cnt
    ,cust_value_cd
    ,email_valid_flg
    ,gender_cd
    ,case when email_address_txt is not null then 'Y' else 'N' end as has_email
    ,ib_status_cd
    ,ibank_register_first_dt 
    ,ibank_register_flg
    ,id_block_flg

    ,job_org_own_bsns_flg
    ,marital_status_cd
    ,monthly_income_amt
    ,pensioner_flg
    ,suppress_limit
    ,suppress_mail_flg
    ,suppress_email_flg
    ,suppress_sms_flg
    ,suppress_call_flg   
   from
    prod_dds.individual_customer
   where
    "YMD_TO_PROCESS" between to_date(valid_from_dttm) and to_date(valid_to_dttm)
  ) cust on cust.customer_rk = f.customer_rk
  left join prod_emart.customer em_cust on em_cust.customer_rk = f.customer_rk
where
 a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;

insert overwrite
 table prod_churn.features_program_product_session partition(ymd)
select
 a.account_rk 
 ,count(distinct case when to_date(sess.session_start_dttm) between date_add(a.ymd, -30) and a.ymd then sess.program_product_cd end) prog_prod_cd_cnt_1m
 ,count(distinct case when to_date(sess.session_start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then sess.program_product_cd end) as prog_prod_cd_cnt_prev_m
 ,count(case when to_date(sess.session_start_dttm) between date_add(a.ymd, -30) and a.ymd then sess.program_product_session_rk end) asprog_prod_sess_cnt_1m
 ,(count(case when to_date(sess.session_start_dttm) between date_add(a.ymd, -30) and a.ymd then sess.program_product_session_rk end)+1)
 /
 (count(case when to_date(sess.session_start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then sess.program_product_session_rk end)+1)
  as prog_prod_sess_cnt_1m_to_prev_m
 ,count(case when to_date(sess.session_start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then sess.program_product_session_rk end) as prog_prod_sess_cnt_prev_m
 ,max(current_timestamp)
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a 
 left join 
  (select 
    customer_rk
	,account_rk
   from 
	prod_dds.customer_x_financial_account f
   where 
	"YMD_TO_PROCESS" between to_date(valid_from_dttm) and to_date(valid_to_dttm)
	and relationship_to_account_cd = 'PRM'
    ) f on a.account_rk = f.account_rk
 left join 
 (select distinct
   customer_rk
   ,session_start_dttm
   ,program_product_cd
   ,program_product_session_rk
  from
   prod_dds.program_product_session 
  where
   to_date(session_start_dttm) >= date_add("YMD_TO_PROCESS", -60)
  ) sess on sess.customer_rk = f.customer_rk
where
 a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;


insert overwrite
 table prod_churn.features_account_chng partition(ymd)
select
 a.account_rk
    ,max(e.past_due_amt) past_due_amt
    ,max((unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(e.curr_period_without_overdue_dt, 'yyyy-MM-dd'))/60/60/24) curr_period_without_overdue_dt
    ,max((unix_timestamp(a.ymd, 'yyyy-MM-dd') - unix_timestamp(e.payed_min_bill_due_dt, 'yyyy-MM-dd'))/60/60/24) payed_min_bill_due_dt
 ,max(current_timestamp)
 ,max("YMD_TO_PROCESS") as ymd
from
 prod_churn.account_to_score a 
 left join 
  (select
    account_rk
    ,past_due_amt
    ,curr_period_without_overdue_dt
    ,payed_min_bill_due_dt
   from
    prod_dds.financial_account_chng
   where
    "YMD_TO_PROCESS" between to_date(effective_from_dttm) and to_date(effective_to_dttm)
   ) e on a.account_rk = e.account_rk  
left join prod_emart.credit_card_account cca on a.account_rk = cca.account_rk  
where
 a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;


insert overwrite
 table prod_churn.features_calls partition(ymd)
select
 a.account_rk
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.COLLECTION_PROCESS_FLG = 'Y' then calls.call_rk end) as call_collection_cnt_1m
 ,sum(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then unix_timestamp(calls.FINISH_DTTM) - unix_timestamp(calls.start_DTTM) end) as call_duration_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'BLC' then calls.call_rk end) as call_res_blc_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'CWI' then calls.call_rk end) as call_res_cwi_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'DFP' then calls.call_rk end) as call_res_dfp_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'DOC' then calls.call_rk end) as call_res_doc_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'ERR' then calls.call_rk end) as call_res_err_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'NDE' then calls.call_rk end) as call_res_nde_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'RPD' then calls.call_rk end) as call_res_rpd_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.call_result_cd = 'STS' then calls.call_rk end) as call_res_sts_cnt_1m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.call_rk end) as calls_cnt_1m
 ,(count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.call_rk end) + 0.5)
  /
  (count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then calls.call_rk end) + 0.5)
  as calls_cnt_1m_to_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) then calls.call_rk end) as calls_cnt_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and call_direction_flg = 'I' then calls.call_rk end) as calls_I_cnt_1m
 ,(count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and call_direction_flg = 'I' then calls.call_rk end) + 0.5)
  /
  (count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and call_direction_flg = 'I' then calls.call_rk end) + 0.5)
  as calls_I_cnt_1m_to_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and call_direction_flg = 'I' then calls.call_rk end) as calls_I_cnt_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and call_direction_flg = 'O' then calls.call_rk end) as calls_O_cnt_1m
 ,(count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and call_direction_flg = 'O' then calls.call_rk end) + 0.5)
  /
  (count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and call_direction_flg = 'O' then calls.call_rk end) + 0.5)
  as calls_O_cnt_1m_to_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -60) and date_add(a.ymd, -30) and call_direction_flg = 'O' then calls.call_rk end) as calls_O_cnt_prev_m
 ,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.INCOMING_CALL_TYPE_CD = 'DRP' then calls.call_rk end) as inc_call_type_drp_cnt_1m
,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.INCOMING_CALL_TYPE_CD = 'NIV' then calls.call_rk end) as inc_call_type_niv_cnt_1m
,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.INCOMING_CALL_TYPE_CD = 'NRM' then calls.call_rk end) as inc_call_type_nrm_cnt_1m
,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.INCOMING_CALL_TYPE_CD = 'SHU' then calls.call_rk end) as inc_call_type_shu_cnt_1m
,count(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd and calls.INCOMING_CALL_TYPE_CD = 'SIV' then calls.call_rk end) as inc_call_type_siv_cnt_1m
,coalesce(max(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then unix_timestamp(calls.FINISH_DTTM) - unix_timestamp(calls.start_DTTM) end), 0) as max_call_duration_1m
,max(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.HUM_TALK_TIME end) as max_HUM_TALK_TIME_1m
,max(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.HUM_WAIT_TIME end) as max_HUM_WAIT_TIME_1m
,max(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.moves_cnt end) as max_moves_cnt_1m
,sum(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.HUM_TALK_TIME end) as sum_HUM_TALK_TIME_1m
,sum(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.HUM_WAIT_TIME end) as sum_HUM_WAIT_TIME_1m
,sum(case when to_date(calls.start_dttm) between date_add(a.ymd, -30) and a.ymd then calls.HUM_TALK_TIME end) as sum_moves_cnt_1m
 ,max(current_timestamp)
,max(a.ymd) as ymd
from 
 prod_churn.account_to_score a
 left join prod_dds.customer_x_financial_account f on a.account_rk = f.account_rk and f.relationship_to_account_cd = 'PRM'
 left join prod_dds.calls calls on calls.customer_rk = f.customer_rk
where
 ((a.ymd between from_unixtime(unix_timestamp(f.valid_from_dttm)) and from_unixtime(unix_timestamp(f.valid_to_dttm))) or (f.valid_to_dttm is null))
 and a.ymd = "YMD_TO_PROCESS"
group by
  a.account_rk
;

insert overwrite
 table prod_churn.features partition(ymd)
select
 cca.account_rk 
,c.age 
,case
  when length(c.auth_type) =  32 then 'SMS-??????????????'
  when length(c.auth_type) =  46 then 'SMS-??? ? SMS-??????????????'
  when length(c.auth_type) =  39 then '?????? ????? ? ??????'
  when length(c.auth_type) =  10 then 'SMS-???'  end as auth_type
,cca.balance_amt 
,calls.call_collection_cnt_1m 
,calls.call_duration_1m 
,calls.call_res_blc_cnt_1m 
,calls.call_res_cwi_cnt_1m 
,calls.call_res_dfp_cnt_1m 
,calls.call_res_doc_cnt_1m 
,calls.call_res_err_cnt_1m 
,calls.call_res_nde_cnt_1m 
,calls.call_res_rpd_cnt_1m 
,calls.call_res_sts_cnt_1m 
,calls.calls_cnt_1m 
,calls.calls_cnt_1m_to_prev_m 
,calls.calls_cnt_prev_m 
,calls.calls_i_cnt_1m 
,calls.calls_i_cnt_1m_to_prev_m 
,calls.calls_i_cnt_prev_m 
,calls.calls_o_cnt_1m 
,calls.calls_o_cnt_1m_to_prev_m 
,calls.calls_o_cnt_prev_m 
,c.car_own_flg 
,c.car_type_flg 
,c.children_cnt 
,a.col_cnt 
,a.core_recently_closed 
,cca.credit_payment_protection_cd 
,t.credit_trans_cnt_1m 
,cca.crl_1m 
,a.cur_cnt 
,ac.curr_period_without_overdue_dt 
,t.currencies_2m 
,cca.current_limit_amt 
,cca.current_to_initial_limit 
,c.cust_value_cd 
,cca.days_open 
,t.days_since_last_credit_trans 
,t.days_since_last_debit_trans 
,cca.days_utilized 
,t.debit_trans_cnt_1m 
,a.deposit_cnt 
,cca.dnl_1m 
,c.education_level_cd 
,cca.email_stmt_send_flg 
,c.email_valid_flg 
,c.gender_cd 
,c.has_email 
,c.ib_status_cd 
,c.ibank_register_days 
,c.ibank_register_flg 
,c.id_block_flg 
,calls.inc_call_type_drp_cnt_1m 
,calls.inc_call_type_niv_cnt_1m 
,calls.inc_call_type_nrm_cnt_1m 
,calls.inc_call_type_shu_cnt_1m 
,calls.inc_call_type_siv_cnt_1m 
,cca.initial_limit_amt 
,c.insider_flg 
,c.job_org_own_bsns_flg 
,a.leg_cnt 
,cca.limit_change_cd 
,cca.limit_utilization 
,a.loan_cnt 
,cca.mail_stmt_send_flg 
,c.marital_status_cd 
,calls.max_call_duration_1m 
,calls.max_hum_talk_time_1m 
,calls.max_hum_wait_time_1m 
,cca.max_limit_amt_1m 
,max_moves_cnt_1m 
,cca.max_to_min_limit_1m 
,cca.min_limit_amt_1m 
,c.monthly_income_amt 
,a.other_accounts_of_customer 
,a.other_active_accounts_of_customer 
,ac.past_due_amt 
,ac.payed_min_bill_due_dt 
,c.pensioner_flg 
,pps.prog_prod_cd_cnt_1m 
,pps.prog_prod_cd_cnt_prev_m 
,pps.prog_prod_sess_cnt_1m 
,pps.prog_prod_sess_cnt_1m_to_prev_m 
,pps.prog_prod_sess_cnt_prev_m 
,a.sav_cnt 
,calls.sum_hum_talk_time_1m 
,calls.sum_hum_wait_time_1m 
,calls.sum_moves_cnt_1m
,c.suppress_flg 
,c.suppress_limit 
,cca.tarif_id 
,t.trans_1m_to_trans_prev_m 
,t.trans_cnt_1m 
,t.trans_cnt_prev_m 
,t.trans_credit_1m_to_trans_prev_m 
,t.trans_credit_sum_1m 
,t.trans_credit_sum_1m_to_trans_sum_prev_m 
,t.trans_credit_sum_prev_m 
,t.trans_debit_1m_to_trans_prev_m 
,t.trans_debit_sum_1m 
,t.trans_debit_sum_1m_to_trans_sum_prev_m 
,t.trans_debit_sum_prev_m
,t.trans_gcs_cnt_1m
,t.trans_sum_1m_to_trans_sum_prev_m 
,t.trans_types_1m 
,current_timestamp
,cca.ymd 
from
 prod_churn.features_credit_card_account cca
 inner join prod_churn.features_account a on cca.account_rk = a.account_rk
 inner join prod_churn.features_trans t on cca.account_rk = t.account_rk
 inner join prod_churn.features_cust c on cca.account_rk = c.account_rk
 inner join prod_churn.features_program_product_session pps on cca.account_rk = pps.account_rk
 inner join prod_churn.features_account_chng ac on cca.account_rk = ac.account_rk
 inner join prod_churn.features_calls calls on cca.account_rk = calls.account_rk
where
 cca.tarif_id not like '6%'
 and cca.ymd = 'YMD_TO_PROCESS'
 and a.ymd = 'YMD_TO_PROCESS'
 and t.ymd = 'YMD_TO_PROCESS'
 and c.ymd = 'YMD_TO_PROCESS'
 and pps.ymd = 'YMD_TO_PROCESS'
 and ac.ymd = 'YMD_TO_PROCESS'
 and calls.ymd = 'YMD_TO_PROCESS'