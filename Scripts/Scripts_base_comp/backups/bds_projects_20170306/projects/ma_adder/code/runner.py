# coding=UTF-8
# Код сверяет дату выгрузки сегментов в SAS MATEMP и дату, к которой относятся сегменты, которые подлежат выгрузке, в таблице с сегментами - prod_lookalike.user_x_segment.
# Если появились более свежие сегменты, запускает выгрузку новых данных.

from subprocess import call
call('touch /home/bigdatasys/projects/ma_adder/code/runner_in_progress', shell = True)

try:

	HIVE_HOST = 'ds-hadoop-cs01p'
	HIVE_PORT = 10000
	HIVE_USER = 'bigdatasys'
	CONF={'hive.vectorized.execution.enabled':'true'
		,'mapreduce.map.memory.mb':'4096'
		,'mapreduce.map.child.java.opts':'-Xmx4g'
		,'mapreduce.task.io.sort.mb':'1024'
		,'mapreduce.reduce.child.java.opts':'-Xmx4g'
		,'mapreduce.reduce.memory.mb':'7000'
		,'mapreduce.reduce.shuffle.input.buffer.percent':'0.5'
		,'mapreduce.input.fileinputformat.split.minsize':'536870912'
		,'mapreduce.input.fileinputformat.split.maxsize':'1073741824'
		,'hive.optimize.ppd':'true'
		,'hive.merge.smallfiles.avgsize':'536870912'
		,'hive.merge.mapredfiles':'true'
		,'hive.merge.mapfiles':'true'
		,'hive.hadoop.supports.splittable.combineinputformat':'true'
		,'hive.exec.reducers.bytes.per.reducer':'536870912'
		,'hive.exec.parallel':'true'
		,'hive.exec.max.created.files':'10000000'
		,'hive.exec.compress.output':'true'
		,'hive.exec.dynamic.partition.mode':'nonstrict'
		,'hive.exec.max.dynamic.partitions':'1000000'
		,'hive.exec.max.dynamic.partitions.pernode':'100000'
		,'io.seqfile.compression.type':'BLOCK'
		,'mapred.reduce.tasks':'1'
			  }

	from pyhive import hive
	conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username=HIVE_USER, configuration=CONF)
	cursor = conn.cursor()

	cursor.execute("""
	select 
	 max(a.ymd) 
	from
	 prod_lookalike.user_x_segment a
	 left semi join prod_lookalike.segments_to_export_to_sas b on a.segment_nm = b.segment_nm
	""")
	max_seg_ymd = cursor.fetchone()[0]
	cursor.execute('select max(ymd) from prod_lookalike.sas_export_dates')
	max_sas_ymd = cursor.fetchone()[0]
	print 'max_sas_ymd = ' + str(max_sas_ymd)
	print 'max_seg_ymd = ' + str(max_seg_ymd)

	import os

	if max_seg_ymd <= max_sas_ymd:
		call('rm /home/bigdatasys/projects/la_segments_builder/new_segments_ready', shell = True)
		call('rm /home/bigdatasys/projects/ma_adder/code/runner_in_progress', shell = True)
                print 'SAS segments up to date'
		cursor.close()
		pid = os.getpid()
		call("kill -9 " + str(pid), shell=True)

	cursor.close()
	print 'executing upload_to_ma.py...'
	execfile('/home/bigdatasys/projects/ma_adder/code/upload_to_ma.py')

except:
    pass

call('rm /home/bigdatasys/projects/ma_adder/code/runner_in_progress', shell = True)