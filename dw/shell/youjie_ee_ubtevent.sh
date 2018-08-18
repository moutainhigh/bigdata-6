#!/bin/bash

#参数:表名 运行模式:1->历史，2->每天一次

#时间戳
#logfile="/data2/gsj/zhuoyi/log/$1.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`



if [[ "$2" == "1" ]];#跑历史数据
then
    years="2017-01-01 2017-01-02 2017-01-03 2017-01-04 2017-01-05 2017-01-06 2017-01-07 2017-01-08 2017-01-09 2017-01-10 2017-01-11 2017-01-12 2017-01-13 2017-01-14 2017-01-15 2017-01-16 2017-01-17 2017-01-18 2017-01-19 2017-01-20 2017-01-21 2017-01-22 2017-01-23 2017-01-24 2017-01-25 2017-01-26 2017-01-27 2017-01-28 2017-01-29 2017-01-30 2017-01-31 2017-02-01 2017-02-02 2017-02-03 2017-02-04 2017-02-05 2017-02-06 2017-02-07 2017-02-08 2017-02-09 2017-02-10 2017-02-11 2017-02-12 2017-02-13 2017-02-14 2017-02-15 2017-02-16 2017-02-17 2017-02-18 2017-02-19 2017-02-20 2017-02-21 2017-02-22 2017-02-23 2017-02-24 2017-02-25 2017-02-26 2017-02-27 2017-02-28 2017-03-01 2017-03-02 2017-03-03 2017-03-04 2017-03-05 2017-03-06 2017-03-07 2017-03-08 2017-03-09 2017-03-10 2017-03-11 2017-03-12 2017-03-13 2017-03-14 2017-03-15 2017-03-16 2017-03-17 2017-03-18 2017-03-19 2017-03-20 2017-03-21 2017-03-22 2017-03-23 2017-03-24 2017-03-25 2017-03-26 2017-03-27 2017-03-28 2017-03-29 2017-03-30 2017-03-31 2017-04-01 2017-04-02 2017-04-03 2017-04-04 2017-04-05 2017-04-06 2017-04-07 2017-04-08 2017-04-09 2017-04-10 2017-04-11 2017-04-12 2017-04-13 2017-04-14 2017-04-15 2017-04-16 2017-04-17 2017-04-18 2017-04-19 2017-04-20 2017-04-21 2017-04-22 2017-04-23 2017-04-24 2017-04-25 2017-04-26 2017-04-27 2017-04-28 2017-04-29 2017-04-30 2017-05-01 2017-05-02 2017-05-03 2017-05-04 2017-05-05 2017-05-06 2017-05-07 2017-05-08 2017-05-09 2017-05-10 2017-05-11 2017-05-12 2017-05-13 2017-05-14 2017-05-15 2017-05-16 2017-05-17 2017-05-18 2017-05-19 2017-05-20 2017-05-21 2017-05-22 2017-05-23 2017-05-24 2017-05-25 2017-05-26 2017-05-27 2017-05-28 2017-05-29 2017-05-30 2017-05-31 2017-06-01 2017-06-02 2017-06-03 2017-06-04 2017-06-05 2017-06-06 2017-06-07 2017-06-08 2017-06-09 2017-06-10 2017-06-11 2017-06-12 2017-06-13 2017-06-14 2017-06-15 2017-06-16 2017-06-17 2017-06-18 2017-06-19 2017-06-20 2017-06-21 2017-06-22 2017-06-23 2017-06-24 2017-06-25 2017-06-26 2017-06-27 2017-06-28 2017-06-29 2017-06-30 2017-07-01 2017-07-02 2017-07-03 2017-07-04 2017-07-05 2017-07-06 2017-07-07 2017-07-08 2017-07-09 2017-07-10 2017-07-11 2017-07-12 2017-07-13 2017-07-14 2017-07-15 2017-07-16 2017-07-17 2017-07-18 2017-07-19 2017-07-20 2017-07-21 2017-07-22 2017-07-23 2017-07-24 2017-07-25 2017-07-26 2017-07-27 2017-07-28 2017-07-29 2017-07-30 2017-07-31 2017-08-01 2017-08-02 2017-08-03 2017-08-04 2017-08-05 2017-08-06 2017-08-07 2017-08-08 2017-08-09 2017-08-10 2017-08-11 2017-08-12 2017-08-13 2017-08-14 2017-08-15 2017-08-16 2017-08-17 2017-08-18 2017-08-19 2017-08-20 2017-08-21 2017-08-22 2017-08-23 2017-08-24 2017-08-25 2017-08-26 2017-08-27 2017-08-28 2017-08-29"
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

            DROP TABLE IF EXISTS tmp.ee_ubtevent_youjie_tmp;
            create table tmp.ee_ubtevent_youjie_tmp as
            select
            concat(domain,timestamp,distinctid,action) dtda,
            max(domain) domain,
            max(distinctid) distinctid,
            max(originalid) originalid,
            max(timestamp)  timestamp,
            max(action)  action,
            max(event) event,
            max(manufacturer) manufacturer,
            max(model) model,
            max(os)  os,
            max(os_version) os_version,
            max(app_version) app_version,
            max(screen_width) screen_width,
            max(screen_height) screen_height,
            max(longitude)  longitude,
            max(latitude)  latitude,
            max(ip) ip,
            max(province)  province,
            max(city) city,
            max(district) district,
            max(market) market,
            max(map_type) map_type,
            max(to_page) to_page,
            max(credit_card_number) credit_card_number,
            max(credit_bank_name)  credit_bank_name,
            max(credit_amount)  credit_amount,
            max(credit_validity) credit_validity,
            max(repay_remind_day) repay_remind_day,
            max(real_name) real_name,
            max(id_number) id_number,
            max(contact_relationship_1) contact_relationship_1,
            max(contact_name_1)  contact_name_1,
            max(contact_phone_1) contact_phone_1,
            max(contact_relationship_2) contact_relationship_2,
            max(contact_name_2)  contact_name_2,
            max(contact_phone_2) contact_phone_2,
            max(job) job,
            max(corporation) corporation,
            max(corporation_address) corporation_address,
            max(education) education,
            max(marry) marry,
            max(debit_card_number) debit_card_number,
            max(debit_bank_name)  debit_bank_name,
            max(loan_amount) loan_amount,
            max(loan_days)  loan_days,
            max(loan_card_number)  loan_card_number,
            max(captcha) captcha,
            max(is_logout) is_logout,
            max(phone_number) phone_number,
            max(addr_detail) addr_detail,
            max(coupon_id) coupon_id,
            max(coupon_name) coupon_name,
            max(mobile)  mobile,
            max(ubtphone) ubtphone,
            max(ubtuserid)  ubtuserid,
            max(pagestaytime) pagestaytime,
            max(productname)  productname,
            max(productsortvalue)  productsortvalue,
            max(bannerurl)  bannerurl,
            max(bannersortvalue) bannersortvalue,
            max(templatename)  templatename,
            max(bannerdesc)  bannerdesc,
            max(bannercode)  bannercode,
            max(entrancetypename)  entrancetypename,
            max(entrancetypecode)  entrancetypecode,
            max(deviceid)  deviceid,
            max(frompage) frompage,
            max(dt) dt,
            max(hour) hour
             from ee.ee_ubtevent where dt='$i' and domain='tkj_youjie' group by concat(domain,timestamp,distinctid,action);

            DROP TABLE IF EXISTS tmp.ubtevent_user_id_trans_tmp;
            create table tmp.ubtevent_user_id_trans_tmp as
            select e.*,substr(from_unixtime(bigint(substr(e.timestamp,0,10))),0,10) as timestamp_trans,
            (case when e.ubtuserid is not null and e.ubtuserid<>'' then e.ubtuserid
                 when e.ubtphone is not null and e.ubtphone<>'' then y.user_id
                 else e.distinctid end) as user_id
            from tmp.ee_ubtevent_youjie_tmp  e
            left join mobp2p.yyd_users y
            on e.ubtphone=y.username;

            insert overwrite table ee.ee_ubtevent_ut PARTITION(dt,hour)
            select
            e.domain                  as domain,
            e.distinctid              as distinctid,
            e.originalid              as originalid,
            e.timestamp               as timestamp,
            e.action                  as action,
            e.event                   as event,
            e.manufacturer            as manufacturer,
            e.model                   as model,
            e.os                      as os,
            e.os_version              as os_version,
            e.app_version             as app_version,
            e.screen_width            as screen_width,
            e.screen_height           as screen_height,
            e.longitude               as longitude,
            e.latitude                as latitude,
            e.ip                      as ip,
            e.province                as province,
            e.city                    as city,
            e.district                as district,
            e.market                  as market,
            e.map_type                as map_type,
            e.to_page                 as to_page,
            e.credit_card_number      as credit_card_number,
            e.credit_bank_name        as credit_bank_name,
            e.credit_amount           as credit_amount,
            e.credit_validity         as credit_validity,
            e.repay_remind_day        as repay_remind_day,
            e.real_name               as real_name,
            e.id_number               as id_number,
            e.contact_relationship_1  as contact_relationship_1,
            e.contact_name_1          as contact_name_1,
            e.contact_phone_1         as contact_phone_1,
            e.contact_relationship_2  as contact_relationship_2,
            e.contact_name_2          as contact_name_2,
            e.contact_phone_2         as contact_phone_2,
            e.job                     as job,
            e.corporation             as corporation,
            e.corporation_address     as corporation_address,
            e.education               as education,
            e.marry                   as marry,
            e.debit_card_number       as debit_card_number,
            e.debit_bank_name         as debit_bank_name,
            e.loan_amount             as loan_amount,
            e.loan_days               as loan_days,
            e.loan_card_number        as loan_card_number,
            e.captcha                 as captcha,
            e.is_logout               as is_logout,
            e.phone_number            as phone_number,
            e.addr_detail             as addr_detail,
            e.coupon_id               as coupon_id,
            e.coupon_name             as coupon_name,
            e.mobile                  as mobile,
            e.ubtphone                as ubtphone,
            e.ubtuserid               as ubtuserid,
            e.pagestaytime            as pagestaytime,
            e.productname             as productname,
            e.productsortvalue        as productsortvalue,
            e.bannerurl               as bannerurl,
            e.bannersortvalue         as bannersortvalue,
            e.templatename            as templatename,
            e.bannerdesc              as bannerdesc,
            e.bannercode              as bannercode,
            e.entrancetypename        as entrancetypename,
            e.entrancetypecode        as entrancetypecode,
            e.deviceid                as deviceid,
            e.frompage                as frompage,
            (case when e.timestamp_trans=y.oe_time then 'new' when e.timestamp_trans<>y.oe_time then 'old' else 'null' end) as user_type,
            y.first_channel           as first_channel,
            e.dt                      as dt,
            e.hour                    as hour
            from tmp.ubtevent_user_id_trans_tmp e
            left join tmp.youjie_first_tmp y
            on e.user_id=y.user_id;"

        echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

    done

else #跑上一天数据
    echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$1 end export"

    echo "`date "+%Y-%m-%d %H:%M:%S"` $1 $i To hive has done "

fi