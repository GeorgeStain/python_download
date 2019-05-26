#!/usr/bin/python3.6
import os
import sys
import datetime
import calendar
from pyhive import presto
import time

PRESTO_HOST = 'presto.wixpress.com'
PRESTO_PORT = 8181
USER = 'georges@wix.com'
spark_path = '/usr/lib/spark2'
sys.path.append(spark_path + "/python/lib/pyspark.zip")
sys.path.append(spark_path + "/python/lib/py4j-0.10.6-src.zip")
os.environ["HADOOP_USER_NAME"] = "wix"

from pyspark.sql import SparkSession
from wix_code_analyzer import WixCodeRunner

# START_TIME / STOP_TIME
# START_DATE_TIME = datetime.date(2017, 7, 24)
# START_TIME = (START_DATE_TIME - datetime.date(1970, 1, 1)).total_seconds() *1000
#
# STOP_DATE_TIME = datetime.date(2018, 1, 1)
# STOP_TIME = (STOP_DATE_TIME - datetime.date(1970, 1, 1)).total_seconds() *1000
# START_TIME / STOP_TIME



def date_trunc(unx_time: int) -> int:
    dt = datetime.datetime.utcfromtimestamp(unx_time/1000).date()
    unx = calendar.timegm(dt.timetuple())
    return unx*1000


mode = ''
first_run = True

if __name__ == "__main__":
    # START_TIME / STOP_TIME
    START_DATE_TIME = datetime.date(2017, 7, 24)
    START_TIME = int((START_DATE_TIME - datetime.date(1970, 1, 1)).total_seconds()) * 1000

    STOP_DATE_TIME = datetime.date(2018, 1, 1)
    STOP_TIME = int((STOP_DATE_TIME - datetime.date(1970, 1, 1)).total_seconds()) * 1000
    # START_TIME / STOP_TIME
    # START_TIME = 1553169600000
    day = (240 * 3600)*1000
    # week = (24 * 3600 * 15)*1000
    # STOP_TIME = 1553428800000

    spark = (SparkSession.builder
        .master("yarn")
        .appName("spark_app_" + os.environ['USER'])
        .config("spark.driver.memory", "5g")
        .config("spark.executor.memory", "5g")
        .config("spark.executor.cores", "2")
        .config("spark.dynamicAllocation.maxExecutors", 200)
        .config('spark.driver.maxResultSize',"5g")
        .config("spark.executor.memoryOverhead", "1g")
        .getOrCreate())


#.config("spark.sql.shuffle.partitions", "3")\
    #
    # cursor = presto.connect(username=USER, host=PRESTO_HOST, port=PRESTO_PORT).cursor()
    # sql_drop = """drop table if exists wixraptor.wix_code.wix_code_site_analyzer_backup19"""
    # sql_create = """create table if not exists wixraptor.wix_code.wix_code_site_analyzer_backup19 as
    #                     select * from  sunduk.tbl.wix_code_site_analyzer_v19"""

    # mode = 'overwrite' if first_run else 'append'
    # WixCodeRunner(spark).execute(date_trunc(START_TIME), date_trunc(STOP_TIME), mode)




    for start_time in range(START_TIME, STOP_TIME, day):
        mode = 'overwrite' if first_run else 'append'
        first_run = False
        WixCodeRunner(spark).execute(date_trunc(start_time), date_trunc(start_time+day), mode)
        # try:
        #     cursor.execute(sql_drop)
        #     cursor.fetchone()
        #     cursor.execute(sql_create)
        #     cursor.fetchone()
        # except Exception as e:
        #     print('failed to back up', e)
        #     pass
        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        print("success: ", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time/1000)),\
              ':',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((start_time+day)/1000)))
    spark.stop()

#/user/wix/wap-repo/sql-tables/wix_code_site_analyzer_v15