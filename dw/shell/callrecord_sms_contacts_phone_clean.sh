#!/bin/bash
#时间戳
tables_name="ncallrecords nsms ncontacts"
years="2014 2015 2016 2017"
mns='01 02 03 04 05 06 07 08 09 10 11 12'


for year in $years
do
    for mn in $mns
    do
        #ncallrecords
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
        insert OVERWRITE table mongo.pq_ncallrecords PARTITION(yr,mn,dt)
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
        yr as yr,mn as mn,dt as dt from mongo.pq_ncallrecords where yr='$year' and mn='$mn';"

        echo "`date "+%Y-%m-%d %H:%M:%S"` ncallrecords year:$year mn:$mn is done================================================================="

        #nsms
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
        insert OVERWRITE table mongo.pq_nsms PARTITION(yr,mn,dt)
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
        yr as yr,mn as mn,dt as dt from mongo.pq_nsms where yr='$year' and mn='$mn';"
        echo "`date "+%Y-%m-%d %H:%M:%S"` nsms year:$year mn:$mn is done================================================================="
        #ncontacts
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
        insert OVERWRITE table mongo.pq_ncontacts PARTITION(yr,mn,dt)
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
        yr as yr,mn as mn,dt as dt from mongo.pq_ncontacts where yr='$year' and mn='$mn';"
        echo "`date "+%Y-%m-%d %H:%M:%S"` ncontacts year:$year mn:$mn is done================================================================="
    done
done