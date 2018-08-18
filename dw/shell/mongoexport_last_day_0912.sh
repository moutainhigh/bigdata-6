#!/bin/bash
#时间戳
HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

logfile="/root/$1.log"

#start=`date -d "2 days ago" +"%Y-%m-%d"`
#mark=`date -d "2 days ago" +"%Y%m%d"`

#start_timestamp=`date -d "$start 00:00:00" +%s`


#end=`date -d "1 days ago" +"%Y-%m-%d"`

#end_timestamp=`date -d "$end 00:00:00" +%s`

#start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
#task_id=`date -d now +"%Y%m%d%H%M%S"`"_pq_$1"

years="2017-09-05 2017-09-06 2017-09-07 2017-09-08 2017-09-09 2017-09-10 2017-09-11"

for i in $years;
do
    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl.jar $i"
    echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile
    start_timestamp=`date -d "$i 00:00:00" +%s`
    end_timestamp=$((start_timestamp+86400))
    if [[ "$1" == "lbs" ]];
    then
        #lbs_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #lbs_task_id=`date -d now +"%Y%m%d%H%M%S"`"_lbs"

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
        select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(lat)>=5 and length(lon)>=5 and length(addtime)=10;"



        echo "`date "+%Y-%m-%d %H:%M:%S"`  $1 To hive has done" >> $logfile


    elif [[ "$1" == "ncallrecords" ]];
    then
        #ncallrecords_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #ncallrecords_task_id=`date -d now +"%Y%m%d%H%M%S"`"_ncallrecords"

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
        select
        user_id user_id,
        addtime addtime,
        count count,
        calltime calltime,
        type type,
        duration duration,
        phone phone,
        name name,
        querytime querytime,
        device_id device_id,
        device_id_type device_id_type,
        mac mac,
        idfa idfa,
        imei imei,
        split(default.phone_clean(phone),'_')[1] phone_clean,
        imsi imsi,
        record_time record_time,
        ac ac,
        fromchannel fromchannel,
        substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"



    elif [[ "$1" == "ncontacts" ]];
    then
        #ncontacts_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #ncontacts_task_id=`date -d now +"%Y%m%d%H%M%S"`"_ncontacts"

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
        select
        user_id user_id,
        addtime addtime,
        tel tel,
        name name,
        querytime querytime,
        device_id device_id,
        device_id_type device_id_type,
        mac mac,
        idfa idfa,
        imei imei,
        deviceupdatetime deviceupdatetime,
        split(default.phone_clean(tel),'_')[1] tel_clean,
        phonetype phonetype,
        count count,
        substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"



    elif [[ "$1" == "nsms" ]];
    then

        #nsms_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #nsms_task_id=`date -d now +"%Y%m%d%H%M%S"`"_nsms"

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
        select
        user_id user_id,
        addtime addtime,
        calltime calltime,
        content content,
        type type,
        phone phone,
        querytime querytime,
        device_id device_id,
        device_id_type device_id_type,
        mac mac,
        idfa idfa,
        imei imei,
        split(default.phone_clean(phone),'_')[1] phone_clean,
        name name,
        count count,
        substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"



    elif [[ "$1" == "mobile" ]];
    then

        #mobile_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #mobile_task_id=`date -d now +"%Y%m%d%H%M%S"`"_mobile"

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
        select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"


    elif [[ "$1" == "mobile_appnames" ]];
    then
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
        insert OVERWRITE table mongo.pq_$1 PARTITION(cal_dt)
        select *,substr(from_unixtime(bigint(addtime)),0,10) as cal_dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"



    else
        #share_userinfo_logs_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #share_userinfo_logs_task_id=`date -d now +"%Y%m%d%H%M%S"`"_share_userinfo_logs"


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
        select *,substr(from_unixtime(bigint(addtime)),0,4) as yr,substr(from_unixtime(bigint(addtime)),6,2) as mn,substr(from_unixtime(bigint(addtime)),9,2) as dt from dw_qlml.$1_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"


        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile
    fi
done

