# coding=UTF-8
# Код выполняет выгрузку кук из сегментов, подлежащих выгрузке в SAS MATEMP, за самую позднюю дату.


from subprocess import call, check_output
from time import sleep

execfile('/home/bigdatasys/spark/00-pyspark-setup.py')


from pyspark import SparkContext, HiveContext
sc = SparkContext()
hc = HiveContext(sc)

max_seg_ymd = hc.sql("""
select
    max(a.ymd)
from
    prod_lookalike.user_x_segment a
    left semi join prod_lookalike.segments_to_export_to_sas b on a.segment_nm = b.segment_nm
""").collect()[0][0]

print '### rm 1'

try:
    call('hadoop fs -rm -r  /prod_lookalike/huids_for_sas_ma', shell = True)
    print '### rm 1 inside'
except:
    pass

hc.sql("""
select distinct
    a.h_uid_rk
from
    prod_lookalike.user_x_segment a
    left semi join prod_lookalike.segments_to_export_to_sas b on a.segment_nm = b.segment_nm
where
    a.ymd = "ymd_dt"
""".replace('ymd_dt', max_seg_ymd)
).rdd.map(lambda x: x.h_uid_rk).saveAsTextFile('/prod_lookalike/huids_for_sas_ma')

print '### Spark part finishing'

sc.stop()

print '### Spark part finished'

file_name = 'ymd_huids.csv'.replace('ymd', max_seg_ymd)

print '### rm 2'

try:
    call('ssh bigdatasys@m1-sasetl01p rm /home/bigdatasys/projects/ma_adder/data/*', shell = True)
    print '### rm 2 inside'
except:
    pass

print '### rm 3'
try:
    call('ssh bigdatasys@m1-sasetl01p rm /home/bigdatasys/projects/ma_adder/success', shell = True)
    print '### rm 3 inside'
except:
    pass

print '### rm 4'
try:
    call('rm /home/bigdatasys/projects/ma_adder/data/*', shell = True)
    print '### rm 4 inside'
except:
    pass

call('hadoop fs -copyToLocal /prod_lookalike/huids_for_sas_ma/part* /home/bigdatasys/projects/ma_adder/data', shell = True)
call('cat /home/bigdatasys/projects/ma_adder/data/* > /home/bigdatasys/projects/ma_adder/data/' + file_name, shell = True)
call('scp /home/bigdatasys/projects/ma_adder/data/' + file_name + ' bigdatasys@m1-sasetl01p:/home/bigdatasys/projects/ma_adder/data', shell = True)


runs = 0

def sas_success():
    try:
        check_output("ssh bigdatasys@m1-sasetl01p cat /home/bigdatasys/projects/ma_adder/success", shell = True)
        return True
    except:
        return False

while runs <=30 and sas_success() == False:
    if runs != 0:
        sleep(1000)
    try:
        call("ssh bigdatasys@m1-sasetl01p /u0/app/SAS_94/config/Lev1/SASChip/BatchServer/sasbatch.sh -sysin /home/bigdatasys/projects/ma_adder/code/import_append.sas -log /home/bigdatasys/projects/ma_adder/log/`date '+20%y.%m.%d-%H-%M-%S'`_import_append.log -ENCODING WCYRILLIC", shell = True)
    except:
        pass
    runs += 1

if sas_success() == False:
    pid = os.getpid()
    call("kill -9 " + str(pid), shell=True)


call('ssh bigdatasys@m1-sasetl01p rm /home/bigdatasys/projects/ma_adder/success', shell = True)

print 'SAS part: SUCCESS'

call('rm /home/bigdatasys/projects/la_segments_builder/new_segments_ready', shell = True)

HIVE_HOST = 'ds-hadoop-cs01p'
HIVE_PORT = 10000
HIVE_USER = 'bigdatasys'
CONF = {'hive.exec.dynamic.partition.mode':'nonstrict'}
from pyhive import hive
conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration = CONF)
cursor = conn.cursor()

query = ("""
insert overwrite 
 table prod_lookalike.sas_export_dates partition(ymd)
select
     segment_nm
    ,"ymd_dt" ymd
from
    prod_lookalike.segments_to_export_to_sas
""".replace('ymd_dt', max_seg_ymd))

print query

cursor.execute(query)

