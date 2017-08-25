# encoding=utf-8

def str_date_ymd(d):
    """возвращает дату d в формате 20161209"""
    from datetime import datetime
    return datetime.strftime(d, "%Y%m%d")

def str_delta_date(d, delta=0):
    """Возвращает дату, отстоящую от d на delta дней вперед, в формате '2016-12-09'"""
    from datetime import datetime, timedelta
    return "'" + datetime.strftime(d + timedelta(days=delta), "%Y-%m-%d") + "'"

def mult_replacement(s, wdict):
    """делает замены в строке s по словарю wdict"""
    for key in wdict:
        s = s.replace(key, wdict[key])
    return s

def exe_quieries(name, host, port, user, conf, queries, wdict=None, start_step=0, end_step=None):
    """
    Подключаемся под юзером user к серверу host:port с конфигурацией conf
    и выполняем последовательность шаблонов запросов queries,
    начиная с шага start_step и до end_step (считая с 0),
    обрабатывая каждый запрос с помощью словаря подстановок wdict
    """
    from pyhive import hive
    from sys import stdout
    from datetime import datetime
    conn = hive.Connection(host=host, port=port, username=user, configuration=conf)
    cursor = conn.cursor()
    
    if end_step is None:
        N = len(queries)
    else:
        N = min(len(queries), end_step+1)
    for i in xrange(start_step, N):
        q = queries[i]
        if wdict is None:
            pass
        else:
            q = mult_replacement(q, wdict)
        print '\n'+name+': Query {0} starting at {1}...'.format(str(i), datetime.now())
        stdout.flush()
        for query in q.split(';'):
            cursor.execute(query)
        print '\n'+name+': Query {0} finished at {1}!'.format(str(i), datetime.now())
        stdout.flush()
    cursor.close()

def create_sample(sample_type, sample_day, sample_length, weblog_term,
                  host, port, user, conf, lib, 
                  clear_tmp=False):
    """
    Этапы формирования выборки от отбора полных заявок до разбиения url'ов веблога
    на фрагменты вида домен#часть_адреса. Если выборка тренировочная, то идет дописывание данных за выбранный период
    в общую таблицу. Если тестовая - создается новая таблица.
    
    Аргументы:
    sample_type - 'train' или 'test' - тип выборки
    sample_day - date - последний день, за который берутся заявки
    sample_length - int - количество дней, за которые берутся заявки
    weblog_term - int - глубина веблога Liveinternet
    host, port, user, conf - параметры соединения Hive
    lib - string - база данных Hive, в которую записываются таблицы
    clear_tmp - boolean - флаг, удалять ли все временные таблицы
    
    Пример:
    crsa.create_sample(sample_type='test', sample_day=datetime(2016,12,10), 
                       sample_length=1, weblog_term=3, 
                       host=HIVE_HOST, port=HIVE_PORT, user=USER, conf=CONF, 
                       lib='user_dmkorzhenkov', clear_tmp=True
                      )
    """
    
    # from datetime import datetime
    
    assert sample_type in ['train', 'test'], "ERROR: sample_type must be 'train' or 'test'!"
    
    # словарь замен в sql-запросах
    WDict = {'#lastdayofsample': str_date_ymd(sample_day),
             '#last-day-of-sample': str_delta_date(sample_day),
             '#samplelength': str(sample_length),
             '#weblogterm': str(weblog_term),
             '#lib': lib
            }  

    hit_date_list = [str_delta_date(sample_day, -i) for i in xrange(sample_length)]
    md5_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                "'a'", "'b'", "'c'", "'d'", "'e'", "'f'"
               ]
    weblog_date_list = [str_delta_date(sample_day, -i) for i in xrange(sample_length+weblog_term-1)]
    
    # подключаем md5
    q1 = 'add jar /opt/cloudera/parcels/hive-extensions/md5app_2.11-1.0.jar'
    q2 = "CREATE FUNCTION  md5 as 'onemd5.Md5'"
    try:
        exe_quieries('md5_creating', host, port, user, conf, [q1, q2], WDict)
        print u'Функция хэширования подключена!'
    except:
        print u'Кажется, функция хэширования уже была подключена...'
        
    # hid всех полных заявок - теперь можно посмотреть, одобрены ли они (в financial_account_application)
    # и куки их первичных версий (в portal_application)
    hid_of_long_init = '''
    drop table if exists 
      #lib.cc_long_hid_#lastdayofsample;
    create table 
      #lib.cc_long_hid_#lastdayofsample 
      (application_id string) 
      partitioned by (hit_date string)
    '''
    hid_of_long = """
    insert into 
      table #lib.cc_long_hid_#lastdayofsample
      partition(hit_date) 
    select 
      post_evar65 as application_id
     ,min(to_date(hit.date_time)) as hit_date
    from prod_raw_site_catalyst.hit_data hit
    where 
         (hit.ymd between date_sub(#last-day-of-sample, #samplelength-1) and date_add(#last-day-of-sample, 1))
     and to_date(hit.date_time) between date_sub(#last-day-of-sample, #samplelength-1) and #last-day-of-sample
     and (hit.prop4 like '/credit/form/%' or hit.prop4 like '/cards/credit-cards/tinkoff-platinum/%') 
     and hit.post_evar64 = 'Tinkoff Platinum'  
     and (   lower(hit.post_evar24) like 'aft%'
          or lower(hit.post_evar24) like 'ctx%'
          or lower(hit.post_evar24) like 'dsp%'
          )  
     and lower(hit.post_evar36) not like '%client%'  
     and (hit.transactionid <> '' and hit.transactionid is not null)
    group by 
     hit.post_evar65
    """    
    exe_quieries('hid_of_long_init', host, port, user, conf, [hid_of_long_init], WDict)
    exe_quieries('hid_of_long', host, port, user, conf, [hid_of_long], WDict)
    
    # смотрим решение по полным заявкам
    decision_on_long_init = '''
    drop table if exists 
      #lib.cc_hid_approval_#lastdayofsample;
    create table 
      #lib.cc_hid_approval_#lastdayofsample 
      (hid string, app_flg bigint)
      partitioned by (hit_date string)
    '''
    decision_on_long = """
    insert into 
      table #lib.cc_hid_approval_#lastdayofsample
      partition(hit_date) 
    select distinct
      a.application_id as hid
     ,(case 
          when b.decision_approve_flg = 'Y' then 1
          when b.decision_reject_flg = 'Y' then 0
          else null
       end
      ) as app_flg
     ,a.hit_date
    from #lib.cc_long_hid_#lastdayofsample a
      inner join (select distinct hid, decision_approve_flg, decision_reject_flg from prod_emart.financial_account_application) b
      on a.application_id = b.hid
         and a.hit_date = #ymd
    """
    exe_quieries('decision_on_long_init', host, port, user, conf, [decision_on_long_init], WDict)
    exe_quieries('decision_on_long', host, port, user, conf, 
                 [decision_on_long.replace('#ymd', d) for d in hit_date_list], 
                 WDict)
    
    # ищем первую портальную куку wuid (в случае, если до конца был заполнен лишь клон) 
    # для полных заявок (их hid) в границах 2 дней до заполнения полной заявки
    first_wuid = '''
    drop table if exists #lib.cc_first_wuid; 
    create table #lib.cc_first_wuid as 
    select 
      t1.hid
     ,(case when t1.linked_id is null then t1.wuid else t2.wuid end) as wuid
    from (select hid, id, linked_id, wuid 
          from prod_dds.portal_application
          where ymd between date_sub(#last-day-of-sample, #samplelength-1) and date_add(#last-day-of-sample, 1)
                and is_processed = 3
         ) t1
       left join 
         (select hid, id, linked_id, wuid 
          from prod_dds.portal_application
          where ymd between date_sub(#last-day-of-sample, #samplelength+1) and date_add(#last-day-of-sample, 1)
         ) t2
       on t1.linked_id = t2.id
    '''    
    exe_quieries('first_wuid', host, port, user, conf, [first_wuid], WDict)
    
    # подтягиваем к полным заявкам первый wuid
    link_hid_wuid_init = '''
    drop table if exists 
      #lib.cc_hid_approval_wuid_#lastdayofsample; 
    create table 
      #lib.cc_hid_approval_wuid_#lastdayofsample 
      (wuid string, hid string, app_flg bigint)
      partitioned by (hit_date string)
    '''
    link_hid_wuid = """
    insert into 
      table #lib.cc_hid_approval_wuid_#lastdayofsample
      partition(hit_date) 
    select  
      b.wuid
     ,a.hid
     ,a.app_flg
     ,a.hit_date
    from #lib.cc_hid_approval_#lastdayofsample a 
      left join #lib.cc_first_wuid b 
      on a.hid = b.hid 
         and a.hit_date = #ymd
    where a.hit_date = #ymd
    """
    exe_quieries('link_hid_wuid_init', host, port, user, conf, [link_hid_wuid_init], WDict)
    exe_quieries('link_hid_wuid', host, port, user, conf, 
                 [link_hid_wuid.replace('#ymd', d) for d in hit_date_list], 
                 WDict)
    
    # оставляем те портальные куки, у кого только один hid
    # (чтобы можно было корректно партиционировать)
    link_wuid_unique_init = '''
    drop table if exists 
      #lib.cc_hid_approval_wuid_unique_#lastdayofsample; 
    create table 
      #lib.cc_hid_approval_wuid_unique_#lastdayofsample 
      (wuid string, app_flg bigint)
      partitioned by (hit_date string)
    '''
    link_wuid_unique = '''
    insert into 
      table #lib.cc_hid_approval_wuid_unique_#lastdayofsample
      partition(hit_date) 
    select  
      a.wuid
     ,a.app_flg
     ,a.hit_date
    from #lib.cc_hid_approval_wuid_#lastdayofsample a
      inner join (
        select t.wuid, count(distinct t.hid) hid_cnt
        from #lib.cc_hid_approval_wuid_#lastdayofsample t
        where t.wuid is not null
        group by t.wuid
        having hid_cnt = 1
      ) b
      on a.wuid = b.wuid
    '''
    exe_quieries('link_wuid_unique_init', host, port, user, conf, [link_wuid_unique_init], WDict)
    exe_quieries('link_wuid_unique', host, port, user, conf, [link_wuid_unique], WDict)
    
    # добавляем куку LiRu
    link_hid_liid_init = '''
    drop table if exists 
      #lib.cc_approval_uid_#lastdayofsample;
    create table 
      #lib.cc_approval_uid_#lastdayofsample 
      (wuid string, app_flg bigint, li_id string)
      partitioned by (hit_date string)
    ;
    create table #lib.tmp_tcs as 
    select distinct 
        source_id, 
        dmp_id 
    from prod_emart.datamind_matching_table 
    where source_type = 'tcs'
    ;
    create table #lib.tmp_liveinternet as
    select distinct 
        source_id, 
        dmp_id 
    from prod_emart.datamind_matching_table 
    where source_type = 'liveinternet'
    '''
    link_hid_liid = """
    insert into 
      table #lib.cc_approval_uid_#lastdayofsample
      partition(hit_date) 
    select distinct
     a.wuid
    ,a.app_flg
    ,c.source_id as li_id
    ,a.hit_date
    from
     #lib.cc_hid_approval_wuid_unique_#lastdayofsample a
     left join #lib.tmp_tcs b 
     on a.hit_date = #ymd 
        and a.wuid = b.source_id
     left join #lib.tmp_liveinternet c 
    on b.dmp_id = c.dmp_id
    where a.hit_date = #ymd 
    """
    link_hid_liid_clear = '''
    drop table if exists 
      #lib.tmp_tcs;
    drop table if exists 
      #lib.tmp_liveinternet  
    '''
    exe_quieries('link_hid_liid_init', host, port, user, conf, [link_hid_liid_init], WDict)
    exe_quieries('link_hid_liid', host, port, user, conf, 
                 [link_hid_liid.replace('#ymd', d) for d in hit_date_list], 
                 WDict)
    exe_quieries('link_hid_liid_clear', host, port, user, conf, [link_hid_liid_clear], WDict)
    
    # отбираем тех, у кого только одна кука LiRu
    having_unique_liid_init = '''
    drop table if exists 
      #lib.cc_approval_liid_unique_#lastdayofsample;
    create table 
      #lib.cc_approval_liid_unique_#lastdayofsample 
      (li_id string, app_flg bigint)
      partitioned by (hit_date string, li_md5 string)
    '''
    having_unique_liid = """
    insert into 
      table #lib.cc_approval_liid_unique_#lastdayofsample
      partition(hit_date, li_md5)
    select 
      a.li_id
     , a.app_flg
     , a.hit_date
     ,substring(md5(li_id), 1, 1) as li_md5
    from 
      #lib.cc_approval_uid_#lastdayofsample a
        inner join (
          select wuid, count(distinct li_id) as li_cnt
          from  #lib.cc_approval_uid_#lastdayofsample
          where app_flg is not null and li_id is not null
          group by wuid
          having li_cnt = 1
          ) b
        on a.wuid = b.wuid
    where a.li_id is not null
    """
    exe_quieries('having_unique_liid_init', host, port, user, conf, [having_unique_liid_init], WDict)
    exe_quieries('having_unique_liid', host, port, user, conf, [having_unique_liid], WDict)
    
    # присоединяем веблог
    step1_train_init = """
    drop table if exists 
      #lib.cc_approved_weblog_step1; 
    create table 
      #lib.cc_approved_weblog_step1 
      (id string, app_flg bigint, domain string, fr array<string>)
      PARTITIONED BY (li_md5 string, weblog_ymd string)
    """
    step1_train = """
    insert into
     table #lib.cc_approved_weblog_step1 
     partition(li_md5, weblog_ymd)
    select
      a.li_id as id,
      a.app_flg,
      b.domain,
      split(parse_url(b.url, 'PATH'), '/') as fr,
      a.li_md5,
      b.ymd as weblog_ymd
    from (
        select *
        from #lib.cc_approval_liid_unique_#lastdayofsample 
        where hit_date = #ymd
      ) a
      left join (
        select 
          uid as id,
          url_domain as domain,
          url,
          ymd
        from prod_odd.weblog
        where ymd between date_sub(#ymd, #weblogterm-1) and #ymd
          and load_src = 'LI.02'
          and url_domain is not null
          and url_domain <> ''
      ) b
      on a.li_id = b.id
    where b.id is not null
    """
    step1_test_init = """
    drop table if exists 
      #lib.cc_approved_weblog_step1_#lastdayofsample; 
    create table 
      #lib.cc_approved_weblog_step1_#lastdayofsample 
      (id string, app_flg bigint, domain string, fr array<string>)
      PARTITIONED BY (li_md5 string, weblog_ymd string)
    """
    step1_test = """
    insert into
     table #lib.cc_approved_weblog_step1_#lastdayofsample 
     partition(li_md5, weblog_ymd)
    select
      a.li_id as id,
      a.app_flg,
      b.domain,
      split(parse_url(b.url, 'PATH'), '/') as fr,
      a.li_md5,
      b.ymd as weblog_ymd
    from (
        select *
        from #lib.cc_approval_liid_unique_#lastdayofsample 
        where hit_date = #ymd
      ) a
      left join (
        select 
          uid as id,
          url_domain as domain,
          url,
          ymd
        from prod_odd.weblog
        where ymd between date_sub(#ymd, #weblogterm-1) and #ymd
          and load_src = 'LI.02'
          and url_domain is not null
          and url_domain <> ''
      ) b
      on a.li_id = b.id
    where b.id is not null
    """
    if sample_type == 'train':
        exe_quieries('step1_train_init', host, port, user, conf, [step1_train_init], WDict)
        exe_quieries('step1_train', host, port, user, conf, 
                     [step1_train.replace('#ymd', d) 
                      for h in md5_list
                     ], WDict)
    elif sample_type == 'test':
        exe_quieries('step1_test_init', host, port, user, conf, [step1_test_init], WDict)
        exe_quieries('step1_test', host, port, user, conf, 
                     [step1_test.replace('#ymd', d) 
                      for d in hit_date_list
                     ], WDict)
    
    # режем на куски
    step3_train_init = """
    drop table if exists 
      #lib.cc_approved_weblog_step3;
    create table 
      #lib.cc_approved_weblog_step3 
      (id string, app_flg bigint, domain string, url_fr string)
      PARTITIONED BY (li_md5 string, weblog_ymd string)
    """
    step3_train = """
    insert into
     table #lib.cc_approved_weblog_step3 
     partition(li_md5, weblog_ymd)
    select 
      id
     ,app_flg
     ,domain
     ,concat(domain, '#', path_fr) as url_fr
     ,li_md5
     ,weblog_ymd
    from #lib.cc_approved_weblog_step1
    LATERAL VIEW explode(fr) tt AS path_fr 
    where 
      li_md5 = #hash
      and domain not like "tinkoff%"
    and weblog_ymd = #ymd
    """
    step3_test_init = """
    drop table if exists 
      #lib.cc_approved_weblog_step3_#lastdayofsample;
    create table 
      #lib.cc_approved_weblog_step3_#lastdayofsample 
      (id string, app_flg bigint, domain string, url_fr string)
      PARTITIONED BY (li_md5 string, weblog_ymd string)
    """
    step3_test = """
    insert into
     table #lib.cc_approved_weblog_step3_#lastdayofsample 
     partition(li_md5, weblog_ymd)
    select 
      id
     ,app_flg
     ,domain
     ,concat(domain, '#', path_fr) as url_fr
     ,li_md5
     ,weblog_ymd
    from #lib.cc_approved_weblog_step1_#lastdayofsample
    LATERAL VIEW explode(fr) tt AS path_fr 
    where 
      li_md5 = #hash
      and domain not like "tinkoff%"
    and weblog_ymd = #ymd
    """
    if sample_type == 'train':
        # exe_quieries('step3_train_init', host, port, user, conf, [step3_train_init], WDict)
        exe_quieries('step3_train', host, port, user, conf, 
                     [step3_train.replace('#ymd', d).replace('#hash', h) 
                      for d in weblog_date_list
                      for h in md5_list
                     ], WDict)
    elif sample_type == 'test':    
        exe_quieries('step3_test_init', host, port, user, conf, [step3_test_init], WDict)
        exe_quieries('step3_test', host, port, user, conf, 
                     [step3_test.replace('#ymd', d).replace('#hash', h) 
                      for d in weblog_date_list
                      for h in md5_list
                     ], WDict)    
    
    
    # удаляем временные таблицы
    clear_temporary = '''
    drop table if exists 
      #lib.#table 
    '''
    tmp_tables = ['cc_long_hid_#lastdayofsample', 
                  'cc_hid_approval_#lastdayofsample',
                  'cc_first_wuid',
                  'cc_hid_approval_wuid_#lastdayofsample',
                  'cc_approval_uid_#lastdayofsample',
                  'cc_hid_approval_wuid_unique_#lastdayofsample',
                  'cc_approval_liid_unique_#lastdayofsample'
                 ]
    if clear_tmp:
        exe_quieries('clear_temporary', host, port, user, conf, 
                     [clear_temporary.replace('#table', t)  
                      for t in tmp_tables 
                     ], WDict)
        
def compute_site_weights(lib, threshold_score, threshold_total, threshold_pos,
                         host, port, user, conf):
    """
    Подсчитывает по тренировочной выборке таблицу весов фрагментов url'ов.
    
    Аргументы:
    lib - string - база данных Hive, в которую записываются таблицы
    threshold_score - float - минимальное значение веса
    threshold_total - int - минимальное значение посетителей фрагмента
    threshold_pos - int - минимальное значение целевых посетителей фрагмента
    host, port, user, conf - параметры соединения Hive
    
    Пример:
    compute_site_weights(lib='user_dmkorzhenkov', 
                         threshold_score=-10000,
                         threshold_total=300,
                         threshold_pos=2,
                         host=HIVE_HOST, port=HIVE_PORT, user=USER, conf=CONF)
    """
    
    WDict = {'#lib': lib,
             '#min_score': str(threshold_score),
             '#min_total': str(threshold_total),
             '#min_pos': str(threshold_pos)
             }
    
    site_weight = '''
    drop table if exists 
      #lib.cc_approve_site_weight;
    create table #lib.cc_approve_site_weight as
    select
      url_fr
      ,log((pos + 0.5)/(total - pos + 0.5))  score
      ,pos
      ,total-pos as neg
      ,total
    from (
     select 
      url_fr
      ,count(distinct id) as total
      ,count(distinct case when app_flg = 1 then id else null end) as pos
      from #lib.cc_approved_weblog_step3
     group by url_fr
    ) tab
    where log((pos + 0.5)/(total - pos + 0.5)) >= #min_score
      and total >= #min_total 
      and pos >= #min_pos
    '''
    
    exe_quieries('site_weight', host, port, user, conf, [site_weight], WDict)
    
def compute_features(lib, sample_type, 
                     host, port, user, conf,
                     sample_day=None, clear_prev=False):
    """
    Подтягивает к фрагментам url'ов их веса и вычисляет признаки для выборки.
    
    Аргументы:
    lib - string - база данных Hive, в которую записываются таблицы
    sample_type - 'train' или 'test' - тип выборки
    sample_day - date - последний день, за который берутся заявки (обязателен для тестовой выборки)
    clear_prev - boolean - флаг, удалять ли таблицу step3
    host, port, user, conf - параметры соединения Hive
    
    Пример:
    compute_features(lib='user_dmkorzhenkov', 
                     sample_type='test', sample_day=datetime(2016,12,10), clear_prev=True,
                     host=HIVE_HOST, port=HIVE_PORT, user=USER, conf=CONF)
    """
    
    assert sample_type in ['train', 'test'], "Invalid sample_type! Must be 'train' or 'test'."
    assert sample_type == 'train' or (sample_type == 'test' and sample_day is not None), "Input sample_day for a test sample!"
    
    WDict = {'#lib': lib,
             '#_lastdayofsample': ('_'+str_date_ymd(sample_day) if sample_type == 'test' else '')
             }
    
    md5_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                "'a'", "'b'", "'c'", "'d'", "'e'", "'f'"
               ]
    
    # добавляем к кускам url-ов вес
    step4_init = '''
    drop table if exists 
      #lib.cc_approved_weblog_step4#_lastdayofsample;
    create table 
      #lib.cc_approved_weblog_step4#_lastdayofsample 
      (id string, app_flg bigint, domain string, url_fr string, score double)
      PARTITIONED BY (li_md5 string, weblog_ymd string)
    '''   
    step4 = '''
    insert into
     table #lib.cc_approved_weblog_step4#_lastdayofsample 
     partition(li_md5, weblog_ymd)
    select
      a.id, 
      a.app_flg, 
      a.domain, 
      a.url_fr, 
      b.score,
      a.li_md5,
      a.weblog_ymd
    from #lib.cc_approved_weblog_step3#_lastdayofsample a
      left join user_dmkorzhenkov.cc_approve_site_weight b
      on a.li_md5 = #hash
         and a.url_fr = b.url_fr
    where a.li_md5 = #hash
    '''         
    exe_quieries('step4_init', host, port, user, conf, [step4_init], WDict)
    exe_quieries('step4', host, port, user, conf, 
                 [step4.replace('#hash', h) 
                  for h in md5_list
                 ], WDict) 
    
    # удаление step3
    clear_step3 = '''
    drop table if exists
      #lib.cc_approved_weblog_step3#_lastdayofsample
    '''
    if clear_prev:
        exe_quieries('clear_step3', host, port, user, conf, [clear_step3], WDict)
    
    # подсчет фич для выборки
    feature_set_init = """
    drop table if exists 
      #lib.cc_approve_features#_lastdayofsample;
    create table 
      #lib.cc_approve_features#_lastdayofsample
      (id string, app_flg bigint, 
       max_score double, min_score double, avg_score double, 
       p90 double, p75 double, p50 double, p25 double, 
       num_url_fr bigint, num_domain bigint, 
       emailru double, moblie_share double, 
       vk_share double, ok_share double, fb_share double, 
       stddev_score double
      ) 
      PARTITIONED BY (li_md5 string)
    """
    feature_set = '''
    insert into
     table #lib.cc_approve_features#_lastdayofsample 
     partition(li_md5)
    select 
      id
     ,app_flg
     ,max(score) as max_score
     ,min(score) as min_score
     ,avg(score) as avg_score
     ,percentile_approx(score,0.90) as p90
     ,percentile_approx(score,0.75) as p75
     ,percentile_approx(score,0.50) as p50
     ,percentile_approx(score,0.25) as p25
     ,count(distinct url_fr) as num_url_fr
     ,count(distinct domain) as num_domain
     ,sum(if(url_fr like 'e.mail.ru%',1,0)) as emailru
     ,sum(if(url_fr like 'm.%',1,0))*1./sum(1) as mobile_share
     ,sum(if(url_fr rlike 'vk\\.com', 1, 0))*1./sum(1) as vk_share
     ,sum(if(url_fr rlike '^(m\\.)?ok\\.ru' or url_fr rlike 'm\\.odnoklassniki\\.ru',1,0))*1./sum(1) as ok_share
     ,sum(if(url_fr rlike '^(m\\.)?fb\\.com' or url_fr rlike 'm\\.facebook\\.com',1,0))*1./sum(1) as fb_share
     ,stddev(score) as stddev_score
     ,li_md5
    from #lib.cc_approved_weblog_step4#_lastdayofsample
    where li_md5 = #hash
    group by id, app_flg, li_md5
    '''
    exe_quieries('feature_set_init', host, port, user, conf, [feature_set_init], WDict)
    exe_quieries('feature_set', host, port, user, conf, 
                 [feature_set.replace('#hash', h) 
                  for h in md5_list
                 ], WDict)