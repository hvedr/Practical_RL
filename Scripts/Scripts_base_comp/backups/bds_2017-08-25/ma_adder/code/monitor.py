# coding=UTF-8
# Процесс, мониторящий возникновение условий для запуска выгрузки сегментов в SAS MATEMP (условия - появились новые сегменты и при этом выгрузка не выполняется в настоящее время).
# Запускается на CRONTAB bigdatasys каждые 10 минут.

from os import path
from time import sleep


if path.exists('/home/bigdatasys/projects/la_segments_builder/new_segments_ready') and not path.exists('/home/bigdatasys/projects/ma_adder/code/runner_in_progress'):
    try:
        execfile('/home/bigdatasys/projects/ma_adder/code/runner.py')	
    except:
        pass