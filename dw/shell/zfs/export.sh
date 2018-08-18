#!/bin/bash
#时间戳
logfile="/root/$1.log"

start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`

echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 begin export ,export date is:$mark" >> $logfile

/root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $1  -q '{addtime:{$gte:'${start_timestamp}',$lt:'${end_timestamp}'}}'  -o /data/$1-$mark.json

hdfs dfs -put /data/$1-$mark.json  /datahouse/ods/mongo/$1/

result=`hdfs dfs -ls /datahouse/ods/mongo/$1 | grep $mark`
if [[ "$result" == *$mark* ]];then
    rm -rf /data/$1-$mark.json
fi

echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 end export" >> $logfile

#spark etl
su hdfs -c "spark-submit --class spark_hive_$1 --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar"
echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile


if [[ "$1" == "lbs" ]];
then
    beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -e "set hive.exec.parallel=true;
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
    select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(lat)>=5 and length(lon)>=5 and length(addtime)=10;"

    echo "`date "+%Y-%m-%d %H:%M:%S"`  $1 To hive has done" >> $logfile
else
    beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -e "set hive.exec.parallel=true;
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
    select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"

    echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile
fi


