#!/bin/bash
#时间戳
logfile="/data2/gsj/log/rule_engine_preanalysis_result.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl
su hdfs -c "spark-submit --class rule_engine_preanalysis_result --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar"
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
insert OVERWRITE table es_rule.biz_rule_engine_preanalysis_result PARTITION(producttype,yr,mn,dt)
select taskId,taskTime,appName,appRequestId,inputParam,outputParam,ruleVersion,producttype as producttype,substr(from_unixtime(bigint($start_timestamp)),0,4) as yr,substr(from_unixtime(bigint($start_timestamp)),6,2) as mn,substr(from_unixtime(bigint($start_timestamp)),9,2) as dt
from es_rule.biz_rule_engine_preanalysis_result_tmp;"

echo "`date "+%Y-%m-%d %H:%M:%S"`  biz_rule_engine_preanalysis_result_tmp To hive has done" >> $logfile
