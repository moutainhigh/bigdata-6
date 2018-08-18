#!/bin/bash
#时间戳
logfile="/data2/gsj/log/bankcard_idcard_validity.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

#spark etl
su hdfs -c "spark-submit --class spark_ocr_identify --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar /data2/zfs/ql_etl.jar 2 1"
&& sudo -u hive hive -e"
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_idcard_validity PARTITION(dt)
select user_id,type,validity_original,issueAuthority_original,address_original,birthday_original,idNumber_original,name_original,people_original,sex_original,status,error,msg,transerialsId,ask_time,ask_return_time,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from rca.yyd_rca_idcard_validity_tmp;" && sudo -u hive hive -e"set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_bankcard_validity PARTITION(dt)
select user_id,cardNumber_original,validate_original,type_original,Issuer_original,status,error,msg,transerialsId,ask_time,ask_return_time,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from rca.yyd_rca_bankcard_validity_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  yyd_rca_bankcard_validity_tmp To hive has done" >> $logfile