# coding=UTF-8
# �������, ����������� ������������� ������� ��� ������� �������� ��������� � SAS MATEMP (������� - ��������� ����� �������� � ��� ���� �������� �� ����������� � ��������� �����).
# ����������� �� CRONTAB bigdatasys ������ 10 �����.

from os import path
from time import sleep


if path.exists('/home/bigdatasys/projects/la_segments_builder/new_segments_ready') and not path.exists('/home/bigdatasys/projects/ma_adder/code/runner_in_progress'):
    try:
        execfile('/home/bigdatasys/projects/ma_adder/code/runner.py')	
    except:
        pass