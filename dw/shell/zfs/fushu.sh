#!/bin/bash
#时间戳
logfile="/data2/gsj/log/fushu.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl
su hdfs -c "sudo -u hdfs spark-submit --class spark_fushu_parse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar  /data2/zfs/ql_etl.jar 2 1"
echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile


sudo -u hive hive -e"
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_fushu_accounts PARTITION(dt)
select *,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from rca.yyd_rca_fushu_accounts_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  yyd_rca_fushu_accounts_tmp To hive has done" >> $logfile


sudo -u hive hive -e"set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_fushu_bills PARTITION(dt)
select *,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from rca.yyd_rca_fushu_bills_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  yyd_rca_fushu_bills_tmp To hive has done" >> $logfile

sudo -u hive hive -e"set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_fushu_flows PARTITION(dt)
select *,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from rca.yyd_rca_fushu_flows_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  yyd_rca_fushu_flows_tmp To hive has done" >> $logfile

