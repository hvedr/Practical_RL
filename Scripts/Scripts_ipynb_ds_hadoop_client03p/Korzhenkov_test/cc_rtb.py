# -*- coding: utf-8 -*-
"""
@author: d.m.korzhenkov
email: d.m.korzhenkov@tinkoff.ru
phone: 1320
"""

from __future__ import print_function
import sys


def str_date_ymd(d, delta=0):
    """
    Возвращает дату, отстоящую от d на delta дней вперед, в формате 20161209
    """

    from datetime import datetime, timedelta
    return datetime.strftime(d + timedelta(days=delta), "%Y%m%d")


def str_delta_date(d, delta=0):
    """
    Возвращает дату, отстоящую от d на delta дней вперед, в формате '2016-12-09'
    """

    from datetime import datetime, timedelta
    return "'" + datetime.strftime(d + timedelta(days=delta), "%Y-%m-%d") + "'"


def mult_replacement(s, wdict):
    """
    делает замены в строке s по словарю wdict
    """

    for key in wdict:
        s = s.replace(key, wdict[key])
    return s


def lift(data, num_parts=1000, verbose=True):
    """
    Возвращает лифт-таблицу для бинарной классификации.
    Переменные лифт-таблицы:
        - % - накопленная доля наблюдений,
        - lift - во сколько раз данная партиция лучше средней по количеству положительных,
        - min_value - минимальное значение вероятности в данной партиции,
        - cnt - количество положительных в данной партиции,
        - % of total pos - отношение количества положительных в данной партиции к общему количеству положительных,
        - p - доля положительных в данной партиции,
        - cumsum - накопленной количество положительных,
        - cumsum % - доля накопленных положительных во всех положительных,
        - total_cumsum - накопленное количество всех наблюдений.

    :param data:
        Массив пар вида (настоящий_класс, вероятность_принадлежности_к_положительному).
        Метки классов должны быть из множества {0, 1}.
        Пример: [(1, 0.57), (0, 0.1), (1, 0.3)]
    :type data: list[tuple]
    :param num_parts:
        Количество партиций для построения лифт-таблицы. Пример: 100
    :type num_parts: int
    :param verbose:
        Флаг, выводить ли в консоль долю положительных среди всех наблюдений.
    :type verbose: bool
    :return: pandas.DataFrame
    """

    import pandas as pd

    data = sorted(data,
                  key=lambda obs: obs[1],
                  reverse=True
                  )

    columns = ('%', 'lift', 'min_score', 'cnt', '% of total pos', 'p', 'cumsum', 'cumsum %', 'total_cumsum')

    num_obs = len(data)  # общее кол-во наблюдений
    num_pos = sum([x[0] for x in data])  # общее количество положительных набдюдений
    if verbose:
        avg1 = 1.0 * num_pos / num_obs
        print('Average share of positives = %f' % avg1)
    res = []
    cum_sum_pos = 0  # накопленное кол-во положительных наблюдений
    cum_sum_obs = 0  # накопленное кол-во всех наблюдений
    for i in range(num_parts):
        part = data[(i * num_obs / num_parts):((i + 1) * num_obs / num_parts)]  # текущая партиция
        part_num_pos = sum([x[0] for x in part])
        part_num_obs = len(part)
        cum_sum_pos += part_num_pos
        cum_sum_obs += part_num_obs
        res.append(((float(i) + 1) * 100 / num_parts,
                    1.0 * part_num_pos * num_parts / num_pos,
                    data[(i + 1) * num_obs / num_parts - 1][1],
                    part_num_pos,
                    100.0 * part_num_pos / num_pos,
                    1.0 * part_num_pos * num_parts / num_obs,
                    cum_sum_pos,
                    100.0 * cum_sum_pos / num_pos,
                    cum_sum_obs
                    )
                   )
    return pd.DataFrame(res, columns=columns)


class CredCardRTB:
    """
    Класс, нацеленный на решение look-alike задачи для кредитных карт Тинькофф Платинум.
    Таргет  - получение заявок, впоследствии одобряемых.
    Класс позволяет создавать тренирововчные и тестовые выборки признаков,
    а также обсчитывать веблог для внедрения модели.
    """

    def __init__(self,
                 date, lib,
                 host, port, username, configuration,
                 length=1, weblog_term=3,
                 verbose=False, output=sys.stdout
                 ):
        """
        Создать экземпляр класса.

        :param date: {datetime}
            Последний день, за который берется выборка. Пример: datetime(2016, 12, 31)
        :param lib: {string}
            Название библиотеки в Hive, в которой создаются таблицы. Пример: 'user_dmkorzhenkov'
        :param host: {string}
            Хост соединения с Hive. Пример: 'ds-hadoop-cs01p'
        :param port: {int}
            Порт соединения с Hive. Пример: 10000
        :param username: {string}
            Пользователь соединения с Hive. Пример: 'd.m.korzhenkov'
        :param configuration: {dictionary}
            Конфигурация соединения с Hive. Пример: {'hive.vectorized.execution.enabled':'true'}
        :param length: {int}
            Количество дней в выборке. Пример: 14
        :param weblog_term: {int}
            Количество дней, за которые анализируется веблог. Пример: 3
        :param verbose: {bool}
            Флаг, подробный ли делать вывод при выполнении запросов.
        :param output: {file object}
            Файл (объект файла) вывода отчета о выполнении запросов.
        """
        self.sample_day = date
        self.lib = lib
        self.sample_length = length
        self.weblog_term = weblog_term
        self.connection = {'host': host,
                           'port': port,
                           'username': username,
                           'configuration': configuration
                           }
        self.score_table = None
        self.score_ymd = None
        self.score_target = None
        self.verbose = verbose
        self.output = output

    def exe_queries(self,
                    queries,
                    name=None, wdict=None,
                    start_step=0, end_step=None,
                    verbose=None
                    ):
        """
        Подключиться к Hive и выполнить последовательность шаблонов запросов,
        обрабатывая каждый шаблон словарем замен.

        :param queries: {array-like of strings}
            Последовательность шаблонов запросов. Пример: ['select * from user_dmkorzhenkov.tmp']
        :param name: {string}
            Имя запроса. Пример: 'create table of applications'
        :param wdict: {dictionary}
            Словарь замен. Пример: {'#weblog_term': str(3)}
        :param start_step: {int}
            Номер запроса в последовательности (считая с 0), с которого начать выполнение. Пример: 7
        :param end_step: {int}
            Номер запроса в последовательности (считая с 0), на котором закончить выполнение. Пример: 9
        :param verbose: {bool}
            Флаг, делать ли вывод о времени выполнения запросов.
        :return:
        """

        from pyhive import hive
        from datetime import datetime

        conn = hive.Connection(**self.connection)
        cursor = conn.cursor()

        if name is None:
            name = ''

        if verbose is None:
            verbose = self.verbose

        if end_step is None:
            n = len(queries)
        else:
            n = min(len(queries), end_step + 1)
        for i in range(start_step, n):
            q = queries[i]
            if wdict is None:
                pass
            else:
                q = mult_replacement(q, wdict)
            if verbose:
                print(name + ': Query {0} starting at {1}...'.format(str(i), datetime.now()),
                      file=self.output
                      )
                self.output.flush()
            for query in q.split(';'):
                cursor.execute(query)
            if verbose:
                print(name + ': Query {0} finished at {1}!'.format(str(i), datetime.now()),
                      file=self.output
                      )
                self.output.flush()
        cursor.close()

    def create_learn_sample(self,
                            sample_type, clear_tmp=True, new_weblog=False
                            ):
        """
        Создать таблицу cc_approved_weblog для обучения/тестирования алгоритмов, содержащую данные о решении по заявке,
        посещенных интернет-сайтах со счетчиками.

        :param sample_type: {'train' or 'test'}
            Тип выборки - тестовая или тренировочная. У тестовой выборки в конце названия таблицы будет суффикс-дата.
            Для тренировочной выборки суффикса не будет, к тому же есть возможность дописывать новые данные в таблицу
            для тренировки.
        :param clear_tmp: {bool}
            Флаг, удалить ли временные таблицы.
        :param new_weblog: {bool}
            Для тренировочной выборки. Флаг, новая ли таблица с выборкой создается или идет дозаписывание в старую.
        :return: (string)
            Название таблицы с подсчитанной статистикой в формате 'имя_библиотеки.имя_таблицы'.
            Пример: 'user_dmkorzhenkov.cc_approved_weblog_20161201'
        """

        assert sample_type in ['train', 'test'], "ERROR: sample_type must be 'train' or 'test'!"

        WDict = {'#_lastdayofsample': ('_' + str_date_ymd(self.sample_day) if sample_type == 'test' else ''),
                 '#last-day-of-sample': str_delta_date(self.sample_day),
                 '#samplelength': str(self.sample_length),
                 '#weblogterm': str(self.weblog_term),
                 '#lib': self.lib
                 }

        hit_date_list = [str_delta_date(self.sample_day, -i) for i in range(self.sample_length)]
        weblog_date_list = [str_delta_date(self.sample_day, -i) for i in
                            range(self.sample_length + self.weblog_term - 1)]
        md5_list = ["'0'", "'1'", "'2'", "'3'", "'4'", "'5'", "'6'", "'7'", "'8'", "'9'",
                    "'a'", "'b'", "'c'", "'d'", "'e'", "'f'"
                    ]

        # подключаем md5
        self.add_md5()

        # hid всех полных заявок - теперь можно посмотреть, одобрены ли они (в financial_account_application)
        # и куки их первичных версий (в portal_application)
        hid_of_long_init = '''
        drop table if exists 
          #lib.cc_long_hid#_lastdayofsample;
        create table 
          #lib.cc_long_hid#_lastdayofsample 
          (application_id string) 
          partitioned by (hit_date string)
        '''
        hid_of_long = """
        insert into 
          table #lib.cc_long_hid#_lastdayofsample
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
        self.exe_queries(name='hid_of_long_init', queries=[hid_of_long_init], wdict=WDict)
        self.exe_queries(name='hid_of_long', queries=[hid_of_long], wdict=WDict)

        # смотрим решение по полным заявкам
        decision_on_long_init = '''
        drop table if exists 
          #lib.cc_hid_approval#_lastdayofsample;
        create table 
          #lib.cc_hid_approval#_lastdayofsample 
          (hid string, app_flg bigint)
          partitioned by (hit_date string)
        '''
        decision_on_long = """
        insert into 
          table #lib.cc_hid_approval#_lastdayofsample
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
        from #lib.cc_long_hid#_lastdayofsample a
          inner join (select distinct hid, decision_approve_flg, decision_reject_flg from prod_emart.financial_account_application) b
          on a.application_id = b.hid
             and a.hit_date = #ymd
        """
        self.exe_queries(name='decision_on_long_init', queries=[decision_on_long_init], wdict=WDict)
        self.exe_queries(name='decision_on_long',
                         queries=[decision_on_long.replace('#ymd', d) for d in hit_date_list],
                         wdict=WDict
                         )

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
        self.exe_queries(name='first_wuid', queries=[first_wuid], wdict=WDict)

        # подтягиваем к полным заявкам первый wuid
        link_hid_wuid_init = '''
        drop table if exists 
          #lib.cc_hid_approval_wuid#_lastdayofsample; 
        create table 
          #lib.cc_hid_approval_wuid#_lastdayofsample 
          (wuid string, hid string, app_flg bigint)
          partitioned by (hit_date string)
        '''
        link_hid_wuid = """
        insert into 
          table #lib.cc_hid_approval_wuid#_lastdayofsample
          partition(hit_date) 
        select  
          b.wuid
         ,a.hid
         ,a.app_flg
         ,a.hit_date
        from #lib.cc_hid_approval#_lastdayofsample a 
          left join #lib.cc_first_wuid b 
          on a.hid = b.hid 
             and a.hit_date = #ymd
        where a.hit_date = #ymd
        """
        self.exe_queries(name='link_hid_wuid_init', queries=[link_hid_wuid_init], wdict=WDict)
        self.exe_queries(name='link_hid_wuid',
                         queries=[link_hid_wuid.replace('#ymd', d) for d in hit_date_list],
                         wdict=WDict
                         )

        # оставляем те портальные куки, у кого только один hid
        # (чтобы можно было корректно партиционировать)
        link_wuid_unique_init = '''
        drop table if exists 
          #lib.cc_hid_approval_wuid_unique#_lastdayofsample; 
        create table 
          #lib.cc_hid_approval_wuid_unique#_lastdayofsample 
          (wuid string, app_flg bigint)
          partitioned by (hit_date string)
        '''
        link_wuid_unique = '''
        insert into 
          table #lib.cc_hid_approval_wuid_unique#_lastdayofsample
          partition(hit_date) 
        select  
          a.wuid
         ,a.app_flg
         ,a.hit_date
        from #lib.cc_hid_approval_wuid#_lastdayofsample a
          inner join (
            select t.wuid, count(distinct t.hid) hid_cnt
            from #lib.cc_hid_approval_wuid#_lastdayofsample t
            where t.wuid is not null
            group by t.wuid
            having hid_cnt = 1
          ) b
          on a.wuid = b.wuid
        '''
        self.exe_queries(name='link_wuid_unique_init', queries=[link_wuid_unique_init], wdict=WDict)
        self.exe_queries(name='link_wuid_unique', queries=[link_wuid_unique], wdict=WDict)

        # добавляем куку LiRu
        link_hid_liid_init = '''
        drop table if exists 
          #lib.cc_approval_uid#_lastdayofsample;
        create table 
          #lib.cc_approval_uid#_lastdayofsample 
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
          table #lib.cc_approval_uid#_lastdayofsample
          partition(hit_date) 
        select distinct
         a.wuid
        ,a.app_flg
        ,c.source_id as li_id
        ,a.hit_date
        from
         #lib.cc_hid_approval_wuid_unique#_lastdayofsample a
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
        self.exe_queries(name='link_hid_liid_init', queries=[link_hid_liid_init], wdict=WDict)
        self.exe_queries(name='link_hid_liid',
                         queries=[link_hid_liid.replace('#ymd', d) for d in hit_date_list],
                         wdict=WDict)
        self.exe_queries(name='link_hid_liid_clear', queries=[link_hid_liid_clear], wdict=WDict)

        # отбираем тех, у кого только одна кука LiRu
        having_unique_liid_init = '''
        drop table if exists 
          #lib.cc_approval_liid_unique#_lastdayofsample;
        create table 
          #lib.cc_approval_liid_unique#_lastdayofsample 
          (li_id string, app_flg bigint)
          partitioned by (hit_date string, li_md5 string)
        '''
        having_unique_liid = """
        insert into 
          table #lib.cc_approval_liid_unique#_lastdayofsample
          partition(hit_date, li_md5)
        select 
          a.li_id
         ,a.app_flg
         ,a.hit_date
         ,substring(md5(li_id), 1, 1) as li_md5
        from 
          #lib.cc_approval_uid#_lastdayofsample a
            inner join (
              select wuid, count(distinct li_id) as li_cnt
              from  #lib.cc_approval_uid#_lastdayofsample
              where app_flg is not null and li_id is not null
              group by wuid
              having li_cnt = 1
              ) b
            on a.wuid = b.wuid
        where a.li_id is not null
        """
        self.exe_queries(name='having_unique_liid_init', queries=[having_unique_liid_init], wdict=WDict)
        self.exe_queries(name='having_unique_liid', queries=[having_unique_liid], wdict=WDict)

        # присоединяем веблог
        liid_weblog_init = """
        drop table if exists 
          #lib.cc_approved_weblog#_lastdayofsample;
        create table 
          #lib.cc_approved_weblog#_lastdayofsample 
          (id string, app_flg bigint, domain string, url_fr string, cnt int, duration int, avg_hour int)
          PARTITIONED BY (li_md5 string, weblog_ymd string)
        """
        liid_weblog = """
        insert into
         table #lib.cc_approved_weblog#_lastdayofsample 
         partition(li_md5, weblog_ymd)
        select
          a.li_id as id
         ,a.app_flg
         ,substr(b.urlfr, 0, instr(b.urlfr,'#')-1) as domain 
         ,b.urlfr as url_fr
         ,b.cnt
         ,b.duration
         ,b.avg_hour
         ,a.li_md5
         ,b.ymd as weblog_ymd
        from 
          #lib.cc_approval_liid_unique#_lastdayofsample a
          inner join prod_features_liveinternet.visits b
          on a.hit_date = #ymd and
             b.ymd = date_sub(#ymd, #delta) and
             a.li_id = b.id
        """
        if new_weblog:
            self.exe_queries(name='liid_weblog_init', queries=[liid_weblog_init], wdict=WDict)
        self.exe_queries(name='liid_weblog',
                         queries=[
                             liid_weblog.replace('#ymd', d).replace('#delta', str(delta))
                             for d in hit_date_list
                             for delta in range(self.weblog_term)
                             ],
                         wdict=WDict
                         )

        # удаляем временные таблицы
        clear_temporary = '''
        drop table if exists 
          #lib.#table 
        '''
        tmp_tables = ['cc_long_hid#_lastdayofsample',
                      'cc_hid_approval#_lastdayofsample',
                      'cc_first_wuid',
                      'cc_hid_approval_wuid#_lastdayofsample',
                      'cc_approval_uid#_lastdayofsample',
                      'cc_hid_approval_wuid_unique#_lastdayofsample',
                      'cc_approval_liid_unique#_lastdayofsample'
                      ]
        if clear_tmp:
            self.exe_queries(name='clear_temporary',
                             queries=[clear_temporary.replace('#table', t) for t in tmp_tables],
                             wdict=WDict
                             )

        # возвращаем название таблицы со статистикой
        return mult_replacement('#lib.cc_approved_weblog#_lastdayofsample', WDict)

    def set_score_properties(self,
                             score_table, score_ymd, score_target
                             ):
        """
        Задать параметры источника скоринга интернет-сайтов.

        :param score_table: {string}
            Таблица в Hive, содержащая скоринг интернет-сайтов.
            Пример: 'prod_features_liveinternet.urlfr_tgt_cnt_cumulative2'
        :param score_ymd: {string in quotes}
            Партиция скоринговой таблицы - дата. Пример: "'2016-12-26'"
        :param score_target: {string in quotes}
            Партиция скоринговой таблицы - целевой класс.
            Пример: "'tinkoff_platinum_approved_application03@tinkoff_action'"
        :return:
        """
        self.score_table = score_table
        self.score_ymd = score_ymd
        self.score_target = score_target

    def compute_features(self,
                         lib_table_name, result_name,
                         threshold_pos=2, threshold_total=300, threshold_score=-10000,
                         svmlib=False, learning_mode=False, clear_tmp=True
                         ):
        """
        Подсчитать признаки для передачи алгоритму либо для обучения/тестирования (learning_mode=True),
        либо для построения прогноза (learning_mode=False).
        Эти два режима отличаются тем, известна ли целевая переменная.
        Подразумевается, что входная таблица имеет структуру
            id, [app_flg (если learning_mode=True),] domain,
            url_fr, cnt, duration, avg_hour, li_md5, weblog_ymd.

        :param lib_table_name: {string}
            Имя входной таблицы с указанием библитеки. Пример: 'user_dmkorzhenkov.cc_approved_weblog'
        :param result_name: {string}
            Имя итоговой таблицы. Пример: 'cc_approve_features'
        :param threshold_pos: {int}
            Порог отсечения сайтов по количеству положительных посетителей. Пример: 2
        :param threshold_total: {int}
            Порог отсечения сайтов по количеству всех посетителей. Пример: 300
        :param threshold_score: {float}
            Порог отсечения сайтов скорингу. Пример: -10.42
        :param svmlib: {bool}
            Флаг, создать ли дополнительно таблицу в svmlib-формате (пригодном для xgboost).
            Имя таблицы будет совпадать с result_name с добавлением суффикса '_svmlib'.
        :param learning_mode: {bool}
            Флаг, используется ли режим подбора модели или обсчитывания веблога для практического применения.
        :param clear_tmp: {bool}
            Флаг, удалить ли временные таблицы.
        :return: {tuple of strings}
            Имена результирующих таблиц без указания библиотеки.
            Если svmlib=False, вернется вектор только с result_name,
            если svmlib=True, вернется вектор с result_name, именем svmlib-таблицы признаков и именем таблицы с uid.
            Пример (при svmlib=True): ('featured_weblog', 'featured_weblog_svmlib', 'featured_weblog_uid')
        """

        if (self.score_table is None) or (self.score_ymd is None) or (self.score_target is None):
            print(u'Call set_score_properties() method before!')
            return

        WDict = {'#app_flg_field': (',app_flg ' if learning_mode else ''),
                 '#app_flg_type': (',app_flg bigint ' if learning_mode else ''),
                 '#app_flg_svm': ('cast(app_flg as string),' if learning_mode else ''),
                 '#score_table': self.score_table,
                 '#score_ymd': self.score_ymd,
                 '#score_target': self.score_target,
                 '#given_table': lib_table_name,
                 '#result_name': result_name,
                 '#pos': str(threshold_pos),
                 '#lib': self.lib,
                 '#total': str(threshold_total),
                 '#score_thres': str(threshold_score)
                 }

        md5_list = ["'0'", "'1'", "'2'", "'3'", "'4'", "'5'", "'6'", "'7'", "'8'", "'9'",
                    "'a'", "'b'", "'c'", "'d'", "'e'", "'f'"
                    ]

        # присвоим сайтам веса         
        scored_weblog_init = """
        drop table if exists 
          #lib.#result_name_prep;
        create table 
          #lib.#result_name_prep 
          (id string #app_flg_type , domain string, url_fr string, cnt int, duration int, avg_hour int, score double)
          PARTITIONED BY (li_md5 string, weblog_ymd string)
        """
        scored_weblog = """
        insert into
         table #lib.#result_name_prep 
         partition(li_md5, weblog_ymd)
        select
          a.id
         #app_flg_field
         ,a.domain
         ,a.url_fr
         ,a.cnt
         ,a.duration
         ,a.avg_hour
         ,b.score
         ,a.li_md5
         ,a.weblog_ymd
        from #given_table a
          inner join #score_table b
          on a.li_md5 = #hash and
             b.ymd = #score_ymd and
             b.target = #score_target and
             b.cnt_positive >= #pos and 
             b.cnt_total >= #total and 
             b.score >= #score_thres and
             a.url_fr = b.urlfr
        where a.li_md5 = #hash
        """
        self.exe_queries(name='scored_weblog_init', queries=[scored_weblog_init], wdict=WDict)
        self.exe_queries(name='scored_weblog',
                         queries=[scored_weblog.replace('#hash', h) for h in md5_list],
                         wdict=WDict
                         )

        # рассчитаем признаки для модели
        features_init = """
        drop table if exists 
          #lib.#result_name;
        create table 
          #lib.#result_name 
          (id string #app_flg_type ,
           max_score double, min_score double, avg_score double, 
           p90 double, p75 double, p50 double, p25 double, 
           num_url_fr bigint, num_domain bigint, 
           emailru double, mobile_share double, 
           vk_share double, ok_share double, fb_share double, 
           stddev_score double,
           hits int,
           max_duration int, min_duration int, avg_duration double,
           max_avg_hour int, min_avg_hour int, avg_avg_hour double
          )
          PARTITIONED BY (li_md5 string)
        """
        features = """
        insert into
         table #lib.#result_name 
         partition(li_md5)
        select
          id
         #app_flg_field
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
         ,sum(cnt) as hits
         ,max(duration) as max_duration
         ,min(duration) as min_duration
         ,avg(duration) as avg_duration
         ,max(avg_hour) as max_avg_hour
         ,min(avg_hour) as min_avg_hour
         ,avg(avg_hour) as avg_avg_hour
         ,li_md5
        from #lib.#result_name_prep 
        where li_md5 = #hash
        group by id #app_flg_field ,li_md5
        """
        self.exe_queries(name='features_init', queries=[features_init], wdict=WDict)
        self.exe_queries(name='features',
                         queries=[features.replace('#hash', h) for h in md5_list],
                         wdict=WDict
                         )

        if clear_tmp:
            clear_tab = 'drop table if exists #lib.#result_name_prep'
            self.exe_queries(name='clear_tmp', queries=[clear_tab], wdict=WDict)

        # создаем svmlib-таблицы для xgboost
        svmlib_make = """
        drop table if exists
          #lib.#result_name_svmlib
        ;
        create table 
          #lib.#result_name_svmlib as 
        select
          concat_ws(
            ' ',
            #app_flg_svm
            concat('0:',  cast(max_score as string)),
            concat('1:',  cast(min_score as string)),
            concat('2:',  cast(avg_score as string)),
            concat('3:',  cast(p90 as string)),
            concat('4:',  cast(p75 as string)),
            concat('5:',  cast(p50 as string)),
            concat('6:',  cast(p25 as string)),
            concat('7:',  cast(num_url_fr as string)),
            concat('8:',  cast(num_domain as string)),
            concat('9:',  cast(emailru as string)),
            concat('10:', cast(mobile_share as string)),
            concat('11:', cast(vk_share as string)),
            concat('12:', cast(ok_share as string)),
            concat('13:', cast(fb_share as string)),
            concat('14:', cast(stddev_score as string)),
            concat('15:', cast(hits as string)),
            concat('16:', cast(max_duration as string)),
            concat('17:', cast(min_duration as string)),
            concat('18:', cast(avg_duration as string)),
            concat('19:', cast(max_avg_hour as string)),
            concat('20:', cast(min_avg_hour as string)),
            concat('21:', cast(avg_avg_hour as string))
          )
        from #lib.#result_name
        """
        uid_make = """
        drop table if exists
          #lib.#result_name_uid
        ;
        create table
          #lib.#result_name_uid as
        select id
        from #lib.#result_name
        """
        if svmlib:
            self.exe_queries(name='make_svmlib', queries=[svmlib_make], wdict=WDict)
            self.exe_queries(name='make_uid', queries=[uid_make], wdict=WDict)

        if svmlib:
            return (mult_replacement('#result_name', WDict),
                    mult_replacement('#result_name_svmlib', WDict),
                    mult_replacement('#result_name_uid', WDict)
                    )
        else:
            return mult_replacement('#result_name', WDict),

    def create_prod_sample(self, clear_tmp=True):
        """
        Создать выборку для последующего расчета признаков и передачи алгоритму прогнозирования.
        Обсчитывается только один день веблога.

        :param clear_tmp: {bool}
            Флаг, удалить ли временные таблицы.
        :return: (tuple of strings)
            Названия таблиц в формате 'имя_библиотеки.имя_таблицы'.
            Первая таблица: подсчитанная статистика, готовая для расчета признаков.
            Вторая таблица: синхронизация кук Li.Ru с DMP  - столбцы id и dmp_id
        """

        WDict = {'#_lastdayofsample': str_date_ymd(self.sample_day),
                 '#last-day-of-sample': str_delta_date(self.sample_day),
                 '#samplelength': str(self.sample_length),
                 '#weblogterm': str(self.weblog_term),
                 '#lib': self.lib
                 }

        # выберем уникальные куки из попавших в веблог за нужный день
        daily_uid_all = """
        drop table if exists
          #lib.daily_uid_all#_lastdayofsample;
        create table
          #lib.daily_uid_all#_lastdayofsample as
        select distinct id
        from prod_features_liveinternet.visits
        where ymd = #last-day-of-sample
        """

        # отберем тех, кто синхронизировался с DMP в последние 3 дня
        daily_uid_dmp = """
        drop table if exists
          #lib.daily_uid_dmp#_lastdayofsample;
        create table
          #lib.daily_uid_dmp#_lastdayofsample as
        select distinct
          a.id
         ,b.dmp_id
        from #lib.daily_uid_all#_lastdayofsample a
          inner join prod_emart.datamind_matching_table b
          on b.source_type = 'liveinternet' and
             (b.ymd between date_sub(#last-day-of-sample, 2) and #last-day-of-sample) and
             a.id = b.source_id
        """

        # отберем уникальные засинхронизированные куки Li.Ru
        daily_uid_good = """
        drop table if exists
          #lib.daily_uid_good#_lastdayofsample;
        create table
          #lib.daily_uid_good#_lastdayofsample as
        select distinct id
        from #lib.daily_uid_dmp#_lastdayofsample
        """

        self.exe_queries([daily_uid_all], 'daily_uid_all', WDict)
        self.exe_queries([daily_uid_dmp], 'daily_uid_dmp', WDict)
        self.exe_queries([daily_uid_good], 'daily_uid_good', WDict)

        # подключим md5
        self.add_md5()

        # список дней ymd, за которые обрабатываем куки
        weblog_date_list = [str_delta_date(self.sample_day, -i) for i in range(self.weblog_term)]

        # создаем таблицу, на основе которой будут считаться признаки
        daily_weblog_init = """
        drop table if exists
          #lib.daily_weblog#_lastdayofsample;
        create table
          #lib.daily_weblog#_lastdayofsample
          (id string, domain string, url_fr string, cnt int, duration int, avg_hour int)
          PARTITIONED BY (li_md5 string, weblog_ymd string)
        """
        daily_weblog = """
        insert into
         table #lib.daily_weblog#_lastdayofsample
         partition(li_md5, weblog_ymd)
        select
          a.id
         ,substr(a.urlfr, 0, instr(a.urlfr,'#')-1) as domain
         ,a.urlfr as url_fr
         ,a.cnt
         ,a.duration
         ,a.avg_hour
         ,substring(md5(a.id), 1, 1) as li_md5
         ,a.ymd as weblog_ymd
        from prod_features_liveinternet.visits a
        where a.ymd = #ymd
          and a.id in (
            select id from #lib.daily_uid_good#_lastdayofsample
          )
        """
        self.exe_queries([daily_weblog_init], 'daily_weblog_init', WDict)
        self.exe_queries([daily_weblog.replace('#ymd', d) for d in weblog_date_list],
                         'daily_weblog',
                         WDict
                         )

        # удаление таблиц
        delete_tables = """
        drop table if exists
          #lib.#tab
        """
        tmp_tables = ['daily_uid_all#_lastdayofsample',
                      # 'daily_uid_dmp#_lastdayofsample'  # эту таблицу подадим в т.ч. на выход, скорее всего
                      'daily_uid_good#_lastdayofsample'
                      ]
        if clear_tmp:
            self.exe_queries([delete_tables.replace('#tab', t) for t in tmp_tables],
                             'delete_tables',
                             WDict
                             )

        # возвращаем имена таблиц: со статистикой и с синхронизацией DMP
        return (mult_replacement('#lib.daily_weblog#_lastdayofsample', WDict),
                mult_replacement('#lib.daily_uid_dmp#_lastdayofsample', WDict)
                )

    def add_md5(self, verbose=None):
        """
        Подключает функцию хэширования md5 для использования в запросах Hive.

        :param verbose: Флаг, выводить ли в консоль отчет о выполнении запроса.
        :type verbose: bool
        """

        if verbose is None:
            verbose = self.verbose

        q1 = 'add jar /opt/cloudera/parcels/hive-extensions/md5app_2.11-1.0.jar'
        q2 = "CREATE FUNCTION  md5 as 'onemd5.Md5'"
        try:
            self.exe_queries(name='md5_creating', queries=[q1, q2], verbose=verbose)
            if verbose:
                print(u'Hashing plugged!', file=self.output)
                self.output.flush()
        except:
            if verbose:
                print(u'Hashing was already plugged ...', file=self.output)
                self.output.flush()
            pass

    def predict_load(self, table_feature, table_uid, table_result, local_dir, xgb_conf):
        """
        Обработать на виртуальной машине таблицу признаков с помощью
        предобученной модели xgboost и вернуть прогнозы в таблицу Hive.
        Все таблицы Hive размещаются в библиотеке, определенной при создании экземпляра класса.

        :param table_feature: {string}
            Имя таблицы признаков в Hive формата svmlib. Таблица без uid!
            Пример: 'featured_weblog_svmlib'
        :param table_uid: {string}
            Имя таблицы с uid, построчно соответствующими признакам в table_feature.
            Пример: 'featured_weblog_uid'
        :param table_result: {string}
            Имя итоговой таблицы с uid Li.Ru и вероятностью. Привер: 'cc_uid_x_score'
        :param local_dir: {string}
            Название директории на виртуальной машине, в которой находятся файл модели xgboost и файл конфигурации
            и куда будут загружаться данные.
            Пример: './libsvm_samples/'
        :param xgb_conf: {string}
            Название файла конфигурации xgboost. Пример: 'xgb.conf'
        :return:
        """
        import re
        from subprocess import call, check_output

        # найдем, из каких файлов состоит таблица
        lib_path = r'/user/hive/warehouse/' + self.lib + r'.db/'  # hdfs-адрес библиотеки
        table_feature_path = lib_path + table_feature + r'/'  # hdfs-адрес таблицы признаков
        table_uid_path = lib_path + table_uid + r'/'  # hdfs-адрес таблицы с uid

        # запрос на список файлов в директории hdfs, отвечающей таблице признаков
        out = 'hadoop fs -ls ' + table_feature_path

        out = check_output(out, shell=True)  # ответ терминала на команду
        table_files = re.findall(table_feature_path + r'(\w+)\n', out)  # парсим и получаем список файлов
        # Пример: table_files == ['000000_0', '000001_0', '000002_0']

        for part in table_files:
            # перемещаем части таблиц на виртуалку
            # hadoop fs -text /user/hive/warehouse/user_dmkorzhenkov.db/featured_weblog_svmlib/000000_0 > ./libsvm_samples/weblog_features.txt
            feature_part2local = ('hadoop fs -text ' +
                                  table_feature_path +
                                  part +
                                  ' > ' +
                                  local_dir +
                                  'weblog_features.txt'
                                  )
            uid_part2local = ('hadoop fs -text ' +
                              table_uid_path +
                              part +
                              '> ' +
                              local_dir +
                              'weblog_uid.txt'
                              )
            call(feature_part2local, shell=True)
            call(uid_part2local, shell=True)

            # делаем предсказание
            # ~/xgboost/xgboost libsvm_samples/xgb.conf task=pred model_in=libsvm_samples/cc_approve_xgboost.model
            make_pred = ('~/xgboost/xgboost ' +
                         local_dir + xgb_conf +
                         ' task=pred'
                         )
            call(make_pred, shell=True)

            # склеиваем предсказание и куку
            # paste -d "," ./libsvm_samples/weblog_uid.txt ./libsvm_samples/pred.txt > ./libsvm_samples/uid_x_score.txt
            paste = ('paste -d "," ' +
                     local_dir + 'weblog_uid.txt ' +
                     local_dir + 'pred.txt > ' +
                     local_dir + 'uid_x_score.txt'
                     )
            call(paste, shell=True)

            # перемещаем склейку на hdfs
            # hadoop fs -put ./libsvm_samples/uid_x_score.txt /user/hive/warehouse/user_dmkorzhenkov.db/cc_uid_x_score.txt
            res2hdfs = ('hadoop fs -put ' +
                        local_dir + 'uid_x_score.txt ' +
                        lib_path + 'cc_uid_x_score.txt'
                        )
            call(res2hdfs, shell=True)

            # записываем файл в таблицу
            q = """
            load data inpath '#fileres'
            into table #lib.#tabres
            """
            q = q.replace('#lib', self.lib).replace('#tabres', table_result)
            q = q.replace('#fileres', lib_path + 'cc_uid_x_score.txt')
            self.exe_queries(name='load2table', queries=[q])

            # TODO: разобраться с тем, чтобы вначале создать в Hive временную textfile-таблицу,
            # затем циклом засунуть в нее данные, а потом после цикла создать на ее основе table_result
            # в виде sequencefile
