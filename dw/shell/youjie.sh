#!/bin/bash

start=`date -d last-day +"%Y-%m-%d"`
start_month=`date -d last-day +"%Y-%m"`


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

            DROP TABLE IF EXISTS tmp.event_login_union_tmp;
            create table tmp.event_login_union_tmp as
            select user_id as user_id,substr(operation_time,0,19) as oe_time,channel_code as first_channel from youjie_mobp2p.y_login_flow;


            DROP TABLE IF EXISTS tmp.youjie_first_tmp;
            create table tmp.youjie_first_tmp as
            select user_id,substr(oe_time,0,10) as oe_time,first_channel from (select user_id,oe_time,first_channel, row_number() over (partition by user_id order by oe_time asc) as od from tmp.event_login_union_tmp) t where od=1;

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
             from ee.ee_ubtevent where dt='$start' and domain='tkj_youjie' group by concat(domain,timestamp,distinctid,action);

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
            on e.user_id=y.user_id;



            DROP TABLE IF EXISTS tmp.borrow_click_stats_log_youjie_tmp;
            create table tmp.borrow_click_stats_log_youjie_tmp as
            select concat(domain,phone,event,eventtime) dpee,max(domain) domain,max(default.jem(phone)) phone,max(amount) amount,max(period) period,max(addproduct) addproduct,max(event) event,max(marketchannel) marketchannel,max(eventtime) eventtime,max(dt) dt from ee.borrow_click_stats_log where dt='$start_month' and domain='' group by concat(domain,phone,event,eventtime);

            DROP TABLE IF EXISTS tmp.bcsl_user_id_trans_tmp;
            create table tmp.bcsl_user_id_trans_tmp as
            select e.*,
            substr(from_unixtime(bigint(substr(e.eventtime,0,10))),0,10) as timestamp_trans,
            y.user_id as user_id
            from tmp.borrow_click_stats_log_youjie_tmp  e
            left join mobp2p.yyd_users y
            on e.phone=y.username ;

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
            (case when e.timestamp_trans=y.oe_time then 'new' when e.timestamp_trans<>y.oe_time then 'old' else 'null' end) as user_type,
            y.first_channel           as first_channel,
            e.dt                      as dt
            from tmp.bcsl_user_id_trans_tmp e
            left join tmp.youjie_first_tmp y
            on e.user_id=y.user_id;



            DROP TABLE IF EXISTS tmp.common_record_event_log_youjie_tmp;
            create table tmp.common_record_event_log_youjie_tmp as
            select concat(domain,userid,event,eventtime) duee,max(domain) domain,max(userid) userid,max(event) event,max(addproduct) addproduct,max(marketchannel) marketchannel,max(eventtime) eventtime, max(dt) dt from ee.common_record_event_log where dt='$start_month' and domain='' group by concat(domain,userid,event,eventtime);


            DROP TABLE IF EXISTS tmp.crel_user_id_trans_tmp;
            create table tmp.crel_user_id_trans_tmp as
            select e.*,
            substr(from_unixtime(bigint(substr(e.eventtime,0,10))),0,10) as timestamp_trans
            from tmp.common_record_event_log_youjie_tmp e;


            insert overwrite table ee.common_record_event_log_ut PARTITION(dt)
            select
            e.domain                  as domain,
            e.userid                  as userid,
            e.event                  as event,
            e.addproduct              as addproduct,
            e.marketchannel           as marketchannel,
            e.eventtime               as eventtime,
            (case when e.timestamp_trans=y.oe_time then 'new' when e.timestamp_trans<>y.oe_time then 'old' else 'null' end) as user_type,
            y.first_channel           as first_channel,
            e.dt                      as dt
            from tmp.crel_user_id_trans_tmp e
            left join tmp.youjie_first_tmp y
            on e.userid=y.user_id;"

