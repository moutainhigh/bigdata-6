#!/bin/bash
#时间戳
logfile="/data2/gsj/log/rule_engine_preanalysis_result.log"
deal_time=${1:-`date +%F%T -d '-1 days'`}
start=${deal_time:0:10}
# start=`date -d last-day +"%Y-%m-%d"`

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl

start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
task_id=`date -d now +"%Y%m%d%H%M%S"`"_biz_rule_engine_preanalysis_result"


su hdfs -c "spark-submit --class rule_engine_preanalysis_result --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
select taskId,updateTime,appName,appRequestId,inputParam,outputParam,ruleVersion,producttype as producttype,substr(from_unixtime(bigint($start_timestamp)),0,4) as yr,substr(from_unixtime(bigint($start_timestamp)),6,2) as mn,substr(from_unixtime(bigint($start_timestamp)),9,2) as dt
from es_rule.biz_rule_engine_preanalysis_result_tmp;"

if [ $? -eq 0 ]; then
    biz_rule_engine_preanalysis_result_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    biz_rule_engine_preanalysis_result_start_timestamp=`date -d "$start_time" +%s`
    biz_rule_engine_preanalysis_result_end_timestamp=`date -d "$biz_rule_engine_preanalysis_result_end_time" +%s`
    biz_rule_engine_preanalysis_result_cost_time=$(($biz_rule_engine_preanalysis_result_end_timestamp-$biz_rule_engine_preanalysis_result_start_timestamp))

    biz_rule_engine_preanalysis_result_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','topic','es_rule','biz_rule_engine_preanalysis_result','${start_time}','${biz_rule_engine_preanalysis_result_end_time}','${biz_rule_engine_preanalysis_result_cost_time}','1')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${biz_rule_engine_preanalysis_result_insert_sql}"
else
    biz_rule_engine_preanalysis_result_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    biz_rule_engine_preanalysis_result_start_timestamp=`date -d "$start_time" +%s`
    biz_rule_engine_preanalysis_result_end_timestamp=`date -d "$biz_rule_engine_preanalysis_result_end_time" +%s`
    biz_rule_engine_preanalysis_result_cost_time=$(($biz_rule_engine_preanalysis_result_end_timestamp-$biz_rule_engine_preanalysis_result_start_timestamp))

    biz_rule_engine_preanalysis_result_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','topic','es_rule','biz_rule_engine_preanalysis_result','${start_time}','${biz_rule_engine_preanalysis_result_end_time}','${biz_rule_engine_preanalysis_result_cost_time}','0')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${biz_rule_engine_preanalysis_result_insert_sql}"
fi

echo "`date "+%Y-%m-%d %H:%M:%S"`  biz_rule_engine_preanalysis_result_tmp To hive has done" >> $logfile
