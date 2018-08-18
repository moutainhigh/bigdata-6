# !/bin/bash
#时间戳
# 参数:table mode
logfile="/data2/gsj/mongo_2/logs/$1.log"
start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`

current_hour=`date +"%Y-%m-%d %H"`
last_hour=`date -d '-1 hours' +"%Y-%m-%d %H"`
current_hour_timestamp=`date -d "$current_hour:00:00" +%s`
last_hour_timestamp=`date -d "$last_hour:00:00" +%s`
mark1=`date -d '-1 hours' +"%Y%m%d%H"`

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"



if [[ "$2" == "1" ]];#跑历史数据
then
    years="201707 201706 201705 201704 201703 201702 201701 201601 201602 201603 201604 201605 201606 201607 201608 201609 201610 201611 201612 2016ago"
    for i in $years;
    do
        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 1 $i"
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
            select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt from tmp.$1_tmp;"

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

    done

else #跑上一天数据

    echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 begin export ,export date is:$mark" >> $logfile

    start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    task_id=`date -d now +"%Y%m%d%H%M%S"`"_pq_$1"

    if [[ "$1" == "mcookie" || "$1" == "credit100_auth_log" ]];
    then
        /root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $1  -q '{addtime:{$gte:'\"${start_timestamp}\"',$lt:'\"${end_timestamp}\"'}}'  -o /data/$1-$mark.json
    elif [[ "$1" == "xinshen_approve_full_log" ]];
    then
        /root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $1  -o /data/$1-$mark.json
    elif [[ "$1" == "user_action" ]];
    then
        /root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $1  -q '{addtime:{$gte:'${last_hour_timestamp}',$lt:'${current_hour_timestamp}'}}'  -o /data/$1-$mark1.json
    else
        /root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $1  -q '{addtime:{$gte:'${start_timestamp}',$lt:'${end_timestamp}'}}'  -o /data/$1-$mark.json
    fi

    if [[ "$1" == "user_action" ]];
    then
        hdfs dfs -put /data/$1-$mark1.json  /datahouse/ods/mongo/$1/
    else
        hdfs dfs -put /data/$1-$mark.json  /datahouse/ods/mongo/$1/
    fi


    result=`hdfs dfs -ls /datahouse/ods/mongo/$1 | grep $mark`
    if [[ "$result" == *$mark* ]];then
        rm -rf /data/$1-$mark.json
    fi

    result1=`hdfs dfs -ls /datahouse/ods/mongo/$1 | grep $mark1`
    if [[ "$result1" == *$mark1* ]];then
        rm -rf /data/$1-$mark1.json
    fi

    echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 end export" >> $logfile

    #spark etl
    #su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1"
    echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile

    if [[ "$1" == "xinshen_approve_full_log" ]];
    then
        #xinshen_approve_full_log_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #xinshen_approve_full_log_task_id=`date -d now +"%Y%m%d%H%M%S"`"_xinshen_approve_full_log"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
        select *,substr(borrow_nid,0,4) as yr,substr(borrow_nid,5,2) as mn,substr(borrow_nid,7,2) as dt from tmp.$1_tmp;"

        if [ $? -eq 0 ]; then
            xinshen_approve_full_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            xinshen_approve_full_log_start_timestamp=`date -d "$start_time" +%s`
            xinshen_approve_full_log_end_timestamp=`date -d "$xinshen_approve_full_log_end_time" +%s`
            xinshen_approve_full_log_cost_time=$(($xinshen_approve_full_log_end_timestamp-$xinshen_approve_full_log_start_timestamp))

            xinshen_approve_full_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_xinshen_approve_full_log','${start_time}','${xinshen_approve_full_log_end_time}','${xinshen_approve_full_log_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${xinshen_approve_full_log_insert_sql}"
        else
            xinshen_approve_full_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            xinshen_approve_full_log_start_timestamp=`date -d "$start_time" +%s`
            xinshen_approve_full_log_end_timestamp=`date -d "$xinshen_approve_full_log_end_time" +%s`
            xinshen_approve_full_log_cost_time=$(($xinshen_approve_full_log_end_timestamp-$xinshen_approve_full_log_start_timestamp))

            xinshen_approve_full_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_xinshen_approve_full_log','${start_time}','${xinshen_approve_full_log_end_time}','${xinshen_approve_full_log_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${xinshen_approve_full_log_insert_sql}"
        fi

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile
    elif [[ "$1" == "user_action" ]];
    then

        #user_action_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #user_action_task_id=`date -d now +"%Y%m%d%H%M%S"`"_user_action"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
        set hive.exec.parallel.thread.number=32;
        set hive.exec.mode.local.auto=true;
        set hive.exec.mode.local.auto.inputbytes.max=50000000;
        set hive.exec.mode.local.auto.tasks.max=20;
        set mapreduce.reduce.memory.mb=4096;
        set hive.exec.dynamic.partition.mode=nonstrict;
        SET hive.exec.max.dynamic.partitions=100000;
        SET hive.exec.max.dynamic.partitions.pernode=100000;
        set parquet.compression=SNAPPY;
        insert OVERWRITE table mongo.pq_$1 PARTITION(yr,mn,dt,hr)
        select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt,substr(from_unixtime(bigint(substr(addtime,0,10))),12,2) as hr from tmp.$1_tmp where substr(addtime,0,10)>=${last_hour_timestamp} and substr(addtime,0,10)<${current_hour_timestamp};"

        if [ $? -eq 0 ]; then
            user_action_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            user_action_start_timestamp=`date -d "$start_time" +%s`
            user_action_end_timestamp=`date -d "$user_action_end_time" +%s`
            user_action_cost_time=$(($user_action_end_timestamp-$user_action_start_timestamp))

            user_action_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_user_action','${start_time}','${user_action_end_time}','${user_action_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${user_action_insert_sql}"
        else
            user_action_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            user_action_start_timestamp=`date -d "$start_time" +%s`
            user_action_end_timestamp=`date -d "$user_action_end_time" +%s`
            user_action_cost_time=$(($user_action_end_timestamp-$user_action_start_timestamp))

            user_action_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_user_action','${start_time}','${user_action_end_time}','${user_action_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${user_action_insert_sql}"
        fi
        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile

    elif [[ "$1" == "activation" ]];
    then

        #activation_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #activation_task_id=`date -d now +"%Y%m%d%H%M%S"`"_activation"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
        select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt from tmp.$1_tmp where substr(addtime,0,10)>=${start_timestamp} and substr(addtime,0,10)<${end_timestamp};"

        if [ $? -eq 0 ]; then
            activation_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            activation_start_timestamp=`date -d "$start_time" +%s`
            activation_end_timestamp=`date -d "$activation_end_time" +%s`
            activation_cost_time=$(($activation_end_timestamp-$activation_start_timestamp))

            activation_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_activation','${start_time}','${activation_end_time}','${activation_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${activation_insert_sql}"
        else
            activation_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            activation_start_timestamp=`date -d "$start_time" +%s`
            activation_end_timestamp=`date -d "$activation_end_time" +%s`
            activation_cost_time=$(($activation_end_timestamp-$activation_start_timestamp))

            activation_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_activation','${start_time}','${activation_end_time}','${activation_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${activation_insert_sql}"
        fi

    elif [[ "$1" == "xinshen_approve_log" ]];
    then

        #xinshen_approve_log_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #xinshen_approve_log_task_id=`date -d now +"%Y%m%d%H%M%S"`"_xinshen_approve_log"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
        select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt from tmp.$1_tmp where substr(addtime,0,10)>=${start_timestamp} and substr(addtime,0,10)<${end_timestamp};"
        if [ $? -eq 0 ]; then
            xinshen_approve_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            xinshen_approve_log_start_timestamp=`date -d "$start_time" +%s`
            xinshen_approve_log_end_timestamp=`date -d "$xinshen_approve_log_end_time" +%s`
            xinshen_approve_log_cost_time=$(($xinshen_approve_log_end_timestamp-$xinshen_approve_log_start_timestamp))

            xinshen_approve_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_xinshen_approve_log','${start_time}','${xinshen_approve_log_end_time}','${xinshen_approve_log_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${xinshen_approve_log_insert_sql}"
        else
            xinshen_approve_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            xinshen_approve_log_start_timestamp=`date -d "$start_time" +%s`
            xinshen_approve_log_end_timestamp=`date -d "$xinshen_approve_log_end_time" +%s`
            xinshen_approve_log_cost_time=$(($xinshen_approve_log_end_timestamp-$xinshen_approve_log_start_timestamp))

            xinshen_approve_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_xinshen_approve_log','${start_time}','${xinshen_approve_log_end_time}','${xinshen_approve_log_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${xinshen_approve_log_insert_sql}"
        fi
    elif [[ "$1" == "mcookie" ]];
    then

        #mcookie_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #mcookie_task_id=`date -d now +"%Y%m%d%H%M%S"`"_mcookie"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
        select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt from tmp.$1_tmp where substr(addtime,0,10)>=${start_timestamp} and substr(addtime,0,10)<${end_timestamp};"

        if [ $? -eq 0 ]; then
            mcookie_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            mcookie_start_timestamp=`date -d "$start_time" +%s`
            mcookie_end_timestamp=`date -d "$mcookie_end_time" +%s`
            mcookie_cost_time=$(($mcookie_end_timestamp-$mcookie_start_timestamp))

            mcookie_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mcookie','${start_time}','${mcookie_end_time}','${mcookie_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mcookie_insert_sql}"
        else
            mcookie_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            mcookie_start_timestamp=`date -d "$start_time" +%s`
            mcookie_end_timestamp=`date -d "$mcookie_end_time" +%s`
            mcookie_cost_time=$(($mcookie_end_timestamp-$mcookie_start_timestamp))

            mcookie_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_mcookie','${start_time}','${mcookie_end_time}','${mcookie_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${mcookie_insert_sql}"
        fi

    else

        #credit100_auth_log_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
        #credit100_auth_log_task_id=`date -d now +"%Y%m%d%H%M%S"`"_credit100_auth_log"

        su hdfs -c "spark-submit --class mongo_2_data_migrate.spark_hive_$1 --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/mongo_2/jars/ql_etl.jar 2 1" && beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
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
        select *,substr(from_unixtime(bigint(substr(addtime,0,10))),0,4) as yr,substr(from_unixtime(bigint(substr(addtime,0,10))),6,2) as mn,substr(from_unixtime(bigint(substr(addtime,0,10))),9,2) as dt from tmp.$1_tmp where substr(addtime,0,10)>=${start_timestamp} and substr(addtime,0,10)<${end_timestamp};"

        if [ $? -eq 0 ]; then
            credit100_auth_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            credit100_auth_log_start_timestamp=`date -d "$start_time" +%s`
            credit100_auth_log_end_timestamp=`date -d "$credit100_auth_log_end_time" +%s`
            credit100_auth_log_cost_time=$(($credit100_auth_log_end_timestamp-$credit100_auth_log_start_timestamp))

            credit100_auth_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_credit100_auth_log','${start_time}','${credit100_auth_log_end_time}','${credit100_auth_log_cost_time}','1')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${credit100_auth_log_insert_sql}"
        else
            credit100_auth_log_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
            credit100_auth_log_start_timestamp=`date -d "$start_time" +%s`
            credit100_auth_log_end_timestamp=`date -d "$credit100_auth_log_end_time" +%s`
            credit100_auth_log_cost_time=$(($credit100_auth_log_end_timestamp-$credit100_auth_log_start_timestamp))

            credit100_auth_log_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${task_id}','10.15.168.4','mongo','pq_credit100_auth_log','${start_time}','${credit100_auth_log_end_time}','${credit100_auth_log_cost_time}','0')"
            mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${credit100_auth_log_insert_sql}"
        fi

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 To hive has done " >> $logfile
    fi


fi
