#!/bin/bash

#参数:表名 运行模式:1->历史，2->每天一次

#时间戳
#logfile="/data2/gsj/zhuoyi/log/$1.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`



if [[ "$2" == "1" ]];#跑历史数据
then
    years="2017-03-28 2017-03-29 2017-03-30 2017-05-07 2017-05-08 2017-05-09 2017-05-10 2017-05-11 2017-05-12 2017-05-13 2017-05-14 2017-05-15 2017-05-16 2017-05-17 2017-05-18 2017-05-19 2017-05-20 2017-05-21 2017-05-22 2017-05-23 2017-05-24 2017-05-25 2017-05-26 2017-05-27 2017-05-28 2017-05-29 2017-05-30 2017-05-31 2017-06-01 2017-06-02 2017-06-03 2017-06-04 2017-06-05 2017-06-06 2017-06-07 2017-06-08 2017-06-09 2017-06-10 2017-06-11 2017-06-12 2017-06-13 2017-06-14 2017-06-15 2017-06-16 2017-06-17 2017-06-18 2017-06-19 2017-06-20 2017-06-21 2017-06-22 2017-06-23 2017-06-24 2017-06-25 2017-06-26 2017-06-27 2017-06-28 2017-06-29 2017-06-30 2017-07-01 2017-07-02 2017-07-03 2017-07-04 2017-07-05 2017-07-06 2017-07-07 2017-07-08 2017-07-09 2017-07-10 2017-07-11 2017-07-12 2017-07-13 2017-07-14 2017-07-15 2017-07-16 2017-07-17 2017-07-18 2017-07-19 2017-07-20 2017-07-21 2017-07-22 2017-07-23 2017-07-24 2017-07-25 2017-07-26 2017-07-27 2017-07-28 2017-07-29 2017-07-30 2017-07-31 2017-08-01 2017-08-02 2017-08-03 2017-08-04 2017-08-05 2017-08-06 2017-08-07 2017-08-08 2017-08-09 2017-08-10 2017-08-11 2017-08-12 2017-08-13 2017-08-14 2017-08-15 2017-08-16 2017-08-17 2017-08-18"
    #years="2017-01-01"
    for i in $years;
    do

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i spark etl has done"

        beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt  -e "set hive.exec.parallel=true;
            set hive.exec.parallel.thread.number=32;
            set hive.exec.mode.local.auto=true;
            set hive.insert.into.multilevel.dirs=true;
            set hive.exec.mode.local.auto.inputbytes.max=50000000;
            set hive.exec.mode.local.auto.tasks.max=20;
            set mapreduce.reduce.memory.mb=4096;
            set hive.exec.dynamic.partition.mode=nonstrict;
            SET hive.exec.max.dynamic.partitions=100000;
            SET hive.exec.max.dynamic.partitions.pernode=100000;
            set parquet.compression=SNAPPY;

            DROP TABLE IF EXISTS tmp.borrow_click_stats_log_youjie_tmp;
            create table tmp.borrow_click_stats_log_youjie_tmp as
            select domain,default.jem(phone) phone,amount,period,addproduct,event,marketchannel,eventtime,dt from ee.borrow_click_stats_log where dt='$i' and domain='';

            DROP TABLE IF EXISTS tmp.bcsl_user_id_trans_tmp;
            create table tmp.bcsl_user_id_trans_tmp as
            select e.*,
            substr(from_unixtime(bigint(substr(e.eventtime,0,10))),0,10) as timestamp_trans,
            y.user_id as user_id
            from tmp.borrow_click_stats_log_youjie_tmp  e
            left join mobp2p.yyd_users y
            on e.phone=y.username;

            insert overwrite table ee.borrow_click_stats_log_ut PARTITION(dt)
            select
            e.domain                  as domain,
            default.jam(e.phone)      as phone,
            e.amount                  as amount,
            e.period                  as period,
            e.addproduct              as addproduct,
            e.event                   as event,
            e.marketchannel           as marketchannel,
            e.eventtime               as eventtime,
            (case when e.timestamp_trans=y.oe_time then 'new'
                 when e.timestamp_trans<>y.oe_time then 'old'
                 else 'null' end) as user_type,
            e.dt                      as dt
            from tmp.bcsl_user_id_trans_tmp e
            left join tmp.youjie_first_tmp y
            on e.user_id=y.user_id;"

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

    done

else #跑上一天数据
    echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 end export"

    #spark etl
    su hdfs -c "spark-submit --class zhuoyi.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/zhuoyi/jars/ql_etl.jar 2 1"
    echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done"

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