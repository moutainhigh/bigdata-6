#!/bin/bash
#时间戳
logfile="/data2/gsj/log/user_behavior_logparse.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl
su hdfs -c "spark-submit --class user_behavior_logparse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar 2 1"
echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile


beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table mongo.pq_customer_alter_log PARTITION(dt)
select userid,datatype,datalog,addtime,addproduct,addchannel,col_name,col_value,substr(from_unixtime(bigint($start_timestamp)),0,10) as dt
from mongo.customer_alter_log_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  customer_alter_log_tmp To hive has done" >> $logfile
