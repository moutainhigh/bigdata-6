#!/bin/bash

#hour=`date -d now +"%H"`
#if [ "$hour" = "00" ];
#then
#    start=`date -d last-day +"%Y-%m-%d"`
#else
#    start=`date -d now +"%Y-%m-%d"`
#fi

#echo $hour
#echo $start

start=`date -d "2 days ago" +"%Y-%m-%d"`
echo $start

sudo -u hive hive -e "
            set hive.exec.parallel=true;
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

            DROP TABLE IF EXISTS tmp.event_login_union_hour_tmp;
            create table tmp.event_login_union_hour_tmp as
            select user_id as user_id,substr(operation_time,0,19) as oe_time,channel_code as first_channel from youjie_mobp2p.y_login_flow;


            DROP TABLE IF EXISTS tmp.youjie_first_hour_tmp;
            create table tmp.youjie_first_hour_tmp as
            select user_id,substr(oe_time,0,10) as oe_time,first_channel from (select user_id,oe_time,first_channel, row_number() over (partition by user_id order by oe_time asc) as od from tmp.event_login_union_hour_tmp) t where od=1;

            insert overwrite table youjie_mobp2p.y_go_borrow_log_ut PARTITION(cal_dt)
            select e.id as id,
            e.user_id as user_id,
            e.phone as phone,
            e.channel_code as channel_code,
            e.template_id as template_id,
            e.template_name as template_name,
            e.product_id  as product_id,
            e.product_name as product_name,
            e.delete_flag as delete_flag,
            e.create_user as create_user,
            e.create_time as create_time,
            e.update_user as update_user,
            e.update_time as update_time,
            (case when substr(e.create_time,0,10)=y.oe_time then 'new' when substr(e.create_time,0,10)<>y.oe_time then 'old' else 'null' end) as user_type,
            y.first_channel as first_channel,
            substr(e.create_time,0,10) as cal_dt
            from youjie_mobp2p.y_go_borrow_log e
            left join tmp.youjie_first_hour_tmp y
            on e.user_id=y.user_id
            where e.create_time>='$start';


            insert overwrite table youjie_mobp2p.y_login_flow_ut PARTITION(cal_dt)
            select e.id as id,
            e.user_id as user_id,
            e.operation as operation,
            e.phone as phone,
            e.channel_code as channel_code,
            e.operation_time as operation_time,
            e.create_user as create_user,
            e.create_time as create_time,
            e.update_user as update_user,
            e.update_time as update_time,
            (case when substr(e.create_time,0,10)=y.oe_time then 'new' when substr(e.create_time,0,10)<>y.oe_time then 'old' else 'null' end) as user_type,
            y.first_channel as first_channel,
            substr(e.create_time,0,10) as cal_dt
            from youjie_mobp2p.y_login_flow e
            left join tmp.youjie_first_hour_tmp y
            on e.user_id=y.user_id
            where e.create_time>='$start';
            "

