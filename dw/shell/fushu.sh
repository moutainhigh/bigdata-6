#!/bin/bash
#时间戳
logfile="/data2/gsj/log/fushu.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
task_id_yyd_rca_fushu_accounts=`date -d now +"%Y%m%d%H%M%S"`"_yyd_rca_fushu_accounts"
task_id_yyd_rca_fushu_bills=`date -d now +"%Y%m%d%H%M%S"`"_yyd_rca_fushu_bills"
task_id_yyd_rca_fushu_flows=`date -d now +"%Y%m%d%H%M%S"`"_yyd_rca_fushu_flows"

#spark etl
su hdfs -c "spark-submit --class spark_fushu_parse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar  /data2/zfs/ql_etl.jar 2 1" && sudo -u hive hive -e"
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
from rca.yyd_rca_fushu_accounts_tmp;" && sudo -u hive hive -e"set hive.exec.parallel=true;
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
from rca.yyd_rca_fushu_bills_tmp;" && sudo -u hive hive -e"set hive.exec.parallel=true;
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

if [ $? -eq 0 ]; then
    fushu_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    fushu_start_timestamp=`date -d "$start_time" +%s`
    fushu_end_timestamp=`date -d "$fushu_end_time" +%s`
    fushu_cost_time=$(($fushu_end_timestamp-$fushu_start_timestamp))

    yyd_rca_fushu_accounts_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_accounts}','topic','rca','yyd_rca_fushu_accounts','${start_time}','${fushu_end_time}','${fushu_cost_time}','1')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_accounts_insert_sql}"

    yyd_rca_fushu_bills_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_bills}','topic','rca','yyd_rca_fushu_bills','${start_time}','${fushu_end_time}','${fushu_cost_time}','1')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_bills_insert_sql}"

    yyd_rca_fushu_flows_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_flows}','topic','rca','yyd_rca_fushu_flows','${start_time}','${fushu_end_time}','${fushu_cost_time}','1')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_flows_insert_sql}"
else
    fushu_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    fushu_start_timestamp=`date -d "$start_time" +%s`
    fushu_end_timestamp=`date -d "$fushu_end_time" +%s`
    fushu_cost_time=$(($fushu_end_timestamp-$fushu_start_timestamp))

    yyd_rca_fushu_accounts_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_accounts}','topic','rca','yyd_rca_fushu_accounts','${start_time}','${fushu_end_time}','${fushu_cost_time}','0')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_accounts_insert_sql}"

    yyd_rca_fushu_bills_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_bills}','topic','rca','yyd_rca_fushu_bills','${start_time}','${fushu_end_time}','${fushu_cost_time}','0')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_bills_insert_sql}"

    yyd_rca_fushu_flows_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id_yyd_rca_fushu_flows}','topic','rca','yyd_rca_fushu_flows','${start_time}','${fushu_end_time}','${fushu_cost_time}','0')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${yyd_rca_fushu_flows_insert_sql}"
fi

echo "`date "+%Y-%m-%d %H:%M:%S"`  yyd_rca_fushu_flows_tmp To hive has done" >> $logfile