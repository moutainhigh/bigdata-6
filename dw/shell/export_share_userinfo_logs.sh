#!/bin/bash
#时间戳

start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`


years="201703 201702 201701 201601 201602 201603 201604 201605 201606 201607 201608 201609 201610 201611 201612 2016ago"
for i in $years;
do
su hdfs -c "spark-submit --class spark_hive_share_userinfo_logs --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_sul.jar $i"
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
    select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp;"

    echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

done
