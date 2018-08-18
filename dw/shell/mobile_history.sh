#!/bin/bash
#时间戳

start=`date -d last-day +"%Y-%m-%d"`
mark=`date -d last-day +"%Y%m%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`


end=`date -d now +"%Y-%m-%d"`

end_timestamp=`date -d "$end 00:00:00" +%s`


years="201601 201602 201603 201604 201605 201606 201607 201608 201609 201610 201611 201612 2016ago 201701 201702 201703 201704 201705 201706 20170622 20170625 20170626 20170627 20170628 20170629 20170630 20170701 20170702 20170703 20170704 20170705 20170706 20170707 20170708 20170709 20170710 20170711 20170712 20170713 20170714 20170715 20170716 20170717 20170718 20170719 20170720 20170721 20170722 20170723 20170724 20170725 20170726 20170727 20170728 20170729 20170730 20170731 20170801 20170802 20170803 20170804 20170805 20170806 20170807 20170808 20170809 20170810 20170811 20170812 20170813 20170814 20170815 20170816 20170817 20170818 20170819 20170820 20170821 20170822 20170823 20170824 20170825 20170826 20170827 20170828 20170829 20170830 20170831 20170901 20170902 20170903 20170904"

for i in $years;
do
    #spark etl
    su hdfs -c "spark-submit --class spark_hive_mobile_appnames --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl.jar $i"
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
        insert OVERWRITE table mongo.pq_mobile_aqp PARTITION(dt)
        select user_id as user_id,device_id as device_id,type as type,photo_name as photo_name,photo_number as photo_number,qqnumber as qqnumber,appname as appname,from_unixtime(bigint(addtime)) as addtime,substr(from_unixtime(bigint(addtime)),0,10) as dt from dw_qlml.mobile_appnames_tmp where length(addtime)=10;"

    echo "$i has done====================================================================================================================="

done