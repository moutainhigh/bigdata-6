#!/bin/bash
#时间戳
HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

logfile="/root/$1.log"

start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`

start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
task_id=`date -d now +"%Y%m%d%H%M%S"`"_pq_$1"

#spark etl
#su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar"
#echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile


if [[ "$1" == "lbs" ]];
then
    #lbs_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #lbs_task_id=`date -d now +"%Y%m%d%H%M%S"`"_lbs"

    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        lbs_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        lbs_start_timestamp=`date -d "$start_time" +%s`
        lbs_end_timestamp=`date -d "$lbs_end_time" +%s`
        lbs_cost_time=$(($lbs_end_timestamp-$lbs_start_timestamp))

        lbs_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_lbs','${start_time}','${lbs_end_time}','${lbs_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${lbs_insert_sql}"
    else
        lbs_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        lbs_start_timestamp=`date -d "$start_time" +%s`
        lbs_end_timestamp=`date -d "$lbs_end_time" +%s`
        lbs_cost_time=$(($lbs_end_timestamp-$lbs_start_timestamp))

        lbs_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_lbs','${start_time}','${lbs_end_time}','${lbs_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${lbs_insert_sql}"
    fi


    echo "`date "+%Y-%m-%d %H:%M:%S"`  $1 To hive has done" >> $logfile


elif [[ "$1" == "ncallrecords" ]];
then
    #ncallrecords_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #ncallrecords_task_id=`date -d now +"%Y%m%d%H%M%S"`"_ncallrecords"

    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        ncallrecords_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        ncallrecords_start_timestamp=`date -d "$start_time" +%s`
        ncallrecords_end_timestamp=`date -d "$ncallrecords_end_time" +%s`
        ncallrecords_cost_time=$(($ncallrecords_end_timestamp-$ncallrecords_start_timestamp))

        ncallrecords_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_ncallrecords','${start_time}','${ncallrecords_end_time}','${ncallrecords_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ncallrecords_insert_sql}"
    else
        ncallrecords_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        ncallrecords_start_timestamp=`date -d "$start_time" +%s`
        ncallrecords_end_timestamp=`date -d "$ncallrecords_end_time" +%s`
        ncallrecords_cost_time=$(($ncallrecords_end_timestamp-$ncallrecords_start_timestamp))

        ncallrecords_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_ncallrecords','${start_time}','${ncallrecords_end_time}','${ncallrecords_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ncallrecords_insert_sql}"
    fi

elif [[ "$1" == "ncontacts" ]];
then
    #ncontacts_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #ncontacts_task_id=`date -d now +"%Y%m%d%H%M%S"`"_ncontacts"

    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        ncontacts_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        ncontacts_start_timestamp=`date -d "$start_time" +%s`
        ncontacts_end_timestamp=`date -d "$ncontacts_end_time" +%s`
        ncontacts_cost_time=$(($ncontacts_end_timestamp-$ncontacts_start_timestamp))

        ncontacts_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_ncontacts','${start_time}','${ncontacts_end_time}','${ncontacts_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ncontacts_insert_sql}"
    else
        ncontacts_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        ncontacts_start_timestamp=`date -d "$start_time" +%s`
        ncontacts_end_timestamp=`date -d "$ncontacts_end_time" +%s`
        ncontacts_cost_time=$(($ncontacts_end_timestamp-$ncontacts_start_timestamp))

        ncontacts_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_ncontacts','${start_time}','${ncontacts_end_time}','${ncontacts_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ncontacts_insert_sql}"
    fi

elif [[ "$1" == "nsms" ]];
then

    #nsms_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #nsms_task_id=`date -d now +"%Y%m%d%H%M%S"`"_nsms"

    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        nsms_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        nsms_start_timestamp=`date -d "$start_time" +%s`
        nsms_end_timestamp=`date -d "$nsms_end_time" +%s`
        nsms_cost_time=$(($nsms_end_timestamp-$nsms_start_timestamp))

        nsms_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_nsms','${start_time}','${nsms_end_time}','${nsms_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${nsms_insert_sql}"
    else
        nsms_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        nsms_start_timestamp=`date -d "$start_time" +%s`
        nsms_end_timestamp=`date -d "$nsms_end_time" +%s`
        nsms_cost_time=$(($nsms_end_timestamp-$nsms_start_timestamp))

        nsms_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_nsms','${start_time}','${nsms_end_time}','${nsms_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${nsms_insert_sql}"
    fi

elif [[ "$1" == "mobile" ]];
then

    #mobile_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #mobile_task_id=`date -d now +"%Y%m%d%H%M%S"`"_mobile"

    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        mobile_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        mobile_start_timestamp=`date -d "$start_time" +%s`
        mobile_end_timestamp=`date -d "$mobile_end_time" +%s`
        mobile_cost_time=$(($mobile_end_timestamp-$mobile_start_timestamp))

        mobile_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mobile','${start_time}','${mobile_end_time}','${mobile_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mobile_insert_sql}"
    else
        mobile_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        mobile_start_timestamp=`date -d "$start_time" +%s`
        mobile_end_timestamp=`date -d "$mobile_end_time" +%s`
        mobile_cost_time=$(($mobile_end_timestamp-$mobile_start_timestamp))

        mobile_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mobile','${start_time}','${mobile_end_time}','${mobile_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mobile_insert_sql}"
    fi

elif [[ "$1" == "mobile_aqp" ]];
then
    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
    set hive.exec.parallel.thread.number=32;
    set hive.exec.mode.local.auto=true;
    set hive.exec.mode.local.auto.inputbytes.max=50000000;
    set hive.exec.mode.local.auto.tasks.max=20;
    set mapreduce.reduce.memory.mb=4096;
    set hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.max.dynamic.partitions=100000;
    SET hive.exec.max.dynamic.partitions.pernode=100000;
    set parquet.compression=SNAPPY;
    insert OVERWRITE table mongo.pq_$1 PARTITION(dt)
    select user_id as user_id,device_id as device_id,type as type,photo_name as photo_name,photo_number as photo_number,qqnumber as qqnumber,appname as appname,from_unixtime(bigint(addtime)) as addtime,substr(from_unixtime(bigint(addtime)),0,10) as dt from dw_qlml.mobile_appnames_tmp where addtime>=${start_timestamp} and addtime<${end_timestamp} and length(addtime)=10;"

    if [ $? -eq 0 ]; then
        mobile_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        mobile_start_timestamp=`date -d "$start_time" +%s`
        mobile_end_timestamp=`date -d "$mobile_end_time" +%s`
        mobile_cost_time=$(($mobile_end_timestamp-$mobile_start_timestamp))

        mobile_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mobile_appnames','${start_time}','${mobile_end_time}','${mobile_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mobile_insert_sql}"
    else
        mobile_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        mobile_start_timestamp=`date -d "$start_time" +%s`
        mobile_end_timestamp=`date -d "$mobile_end_time" +%s`
        mobile_cost_time=$(($mobile_end_timestamp-$mobile_start_timestamp))

        mobile_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mobile_appnames','${start_time}','${mobile_end_time}','${mobile_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mobile_insert_sql}"
    fi

else
    #share_userinfo_logs_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    #share_userinfo_logs_task_id=`date -d now +"%Y%m%d%H%M%S"`"_share_userinfo_logs"


    su hdfs -c "spark-submit --class spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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

    if [ $? -eq 0 ]; then
        share_userinfo_logs_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        share_userinfo_logs_start_timestamp=`date -d "$start_time" +%s`
        share_userinfo_logs_end_timestamp=`date -d "$share_userinfo_logs_end_time" +%s`
        share_userinfo_logs_cost_time=$(($share_userinfo_logs_end_timestamp-$share_userinfo_logs_start_timestamp))

        share_userinfo_logs_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_share_userinfo_logs','${start_time}','${share_userinfo_logs_end_time}','${share_userinfo_logs_cost_time}','1')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${share_userinfo_logs_insert_sql}"
    else
        share_userinfo_logs_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        share_userinfo_logs_start_timestamp=`date -d "$start_time" +%s`
        share_userinfo_logs_end_timestamp=`date -d "$share_userinfo_logs_end_time" +%s`
        share_userinfo_logs_cost_time=$(($share_userinfo_logs_end_timestamp-$share_userinfo_logs_start_timestamp))

        share_userinfo_logs_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_share_userinfo_logs','${start_time}','${share_userinfo_logs_end_time}','${share_userinfo_logs_cost_time}','0')"
        mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${share_userinfo_logs_insert_sql}"
    fi
    echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile
fi