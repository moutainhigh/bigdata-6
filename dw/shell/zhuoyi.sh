#!/bin/bash

#参数:表名 运行模式:1->历史，2->每天一次

#时间戳
logfile="/data2/gsj/zhuoyi/log/$1.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`



if [[ "$2" == "1" ]];#跑历史数据
then
    years="2017-08-01 2017-08-02 2017-08-03 2017-08-04 2017-08-05"
    for i in $years;
    do
        su hdfs -c "spark-submit --class zhuoyi.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/zhuoyi/jars/ql_etl.jar 1 $i"
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
            insert OVERWRITE table mongo.pq_$1 PARTITION(yr,mn,dt)
            select *,substr(record_time,0,4) as yr,substr(record_time,6,2) as mn,substr(record_time,9,2) as dt from tmp.$1_tmp;"

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

    done

else #跑上一天数据
    echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 end export" >> $logfile

    #spark etl
    su hdfs -c "spark-submit --class zhuoyi.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/zhuoyi/jars/ql_etl.jar 2 1"
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
            insert OVERWRITE table mongo.pq_$1 PARTITION(yr,mn,dt)
            select *,substr(record_time,0,4) as yr,substr(record_time,6,2) as mn,substr(record_time,9,2) as dt from tmp.$1_tmp;"

    echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

fi