import sys
sys.path.append('/home/m.v.surovikov/.ipython/scripts') # путь к файлу со скриптом lookalike_model_builder.py
from lookalike_model_builder import build_la_model, lift

execfile('/home/m.v.surovikov/.ipython/profile_spark/startup/00-pyspark-setup.py') # файл из спарковского профиля ноутбука


build_la_model(ymd_start = '2016-05-25'
              , ymd_end = '2016-05-25'
              , ymd_test = '2016-06-25'
              , cluster_prod = True
              , HIVE_USER = 'mvsurovikov'
              , tinkoff_page = True
              , flag_file = '/home/m.v.surovikov/.ipython/scripts/la_in_use'
              , target_url_like_expression = '(url_str like "%tinkoff.ru/credit/cards/allairlines%" or url_str like "%tinkoff.ru/credit/form/aa%" or url_str like "%tinkoff.ru/iframe/credit/allairlines%") and url_str not like "%agent%"'
              #, target_url_like_expression = '%sravni.ru/karty%'
              , domain_exclude = 'tinkoff.ru'
              , seg_name = 'tcs_aa'
              , unique_sync = True)   

import os
import subprocess

pid = os.getpid()
subprocess.call("kill -9 " + str(pid), shell=True)