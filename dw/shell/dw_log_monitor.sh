# !/bin/bash
#时间戳
# 参数:table mode
logfile="/data2/gsj/dw_log_monitor/logs/$1.log"
start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`



if [[ "$2" == "1" ]];#跑历史数据
then
    years="2017/08/04 2017/08/05 2017/08/06 2017/08/07 2017/08/08 2017/08/09"
    for i in $years;
    do
        su hdfs -c "spark-submit --class dw_log_monitor.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/dw_log_monitor/jars/ql_etl.jar 1 $i"
        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i spark etl has done"

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
            insert OVERWRITE table decision.dw_riskdata_log_monitor PARTITION(yr,mn,dt)
            select user_id as user_id,query_id as query_id,query_string as query_string,submit_time as submit_time,cost_time as cost_time,substr(from_unixtime(bigint(substr(finishTime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(finishTime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(finishTime,0,10))),9,2) as dt from tmp.$1_tmp
            where query_string<>'' and query_string<>'NULL' and query_string is not null and length(query_string)>0;"

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

    done

else #跑上一天数据

    #spark etl
    su hdfs -c "spark-submit --class dw_log_monitor.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/dw_log_monitor/jars/ql_etl.jar 2"
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
        insert OVERWRITE table decision.dw_riskdata_log_monitor PARTITION(yr,mn,dt)
        select user_id as user_id,query_id as query_id,query_string as query_string,submit_time as submit_time,cost_time as cost_time,substr(from_unixtime(bigint(substr(finishTime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(finishTime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(finishTime,0,10))),9,2) as dt from tmp.$1_tmp
        where query_string<>'' and query_string<>'NULL' and query_string is not null and length(query_string)>0;"
fi