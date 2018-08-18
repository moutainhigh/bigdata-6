#!/bin/bash
#时间戳
logfile="/data2/gsj/log/user_behavior_logparse.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"


start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
task_id=`date -d now +"%Y%m%d%H%M%S"`"_pq_customer_alter_log"

su hdfs -c "spark-submit --class user_behavior_logparse --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/zfs/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

if [ $? -eq 0 ]; then
    pq_customer_alter_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    pq_customer_alter_log_start_timestamp=`date -d "$start_time" +%s`
    pq_customer_alter_log_end_timestamp=`date -d "$pq_customer_alter_log_end_time" +%s`
    pq_customer_alter_log_cost_time=$(($pq_customer_alter_log_end_timestamp-$pq_customer_alter_log_start_timestamp))

    pq_customer_alter_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','topic','mongo','pq_customer_alter_log','${start_time}','${pq_customer_alter_log_end_time}','${pq_customer_alter_log_cost_time}','1')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${pq_customer_alter_log_insert_sql}"
else
    pq_customer_alter_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    pq_customer_alter_log_start_timestamp=`date -d "$start_time" +%s`
    pq_customer_alter_log_end_timestamp=`date -d "$pq_customer_alter_log_end_time" +%s`
    pq_customer_alter_log_cost_time=$(($pq_customer_alter_log_end_timestamp-$pq_customer_alter_log_start_timestamp))

    pq_customer_alter_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','topic','mongo','pq_customer_alter_log','${start_time}','${pq_customer_alter_log_end_time}','${pq_customer_alter_log_cost_time}','0')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${pq_customer_alter_log_insert_sql}"
fi

echo "`date "+%Y-%m-%d %H:%M:%S"`  customer_alter_log_tmp To hive has done" >> $logfile