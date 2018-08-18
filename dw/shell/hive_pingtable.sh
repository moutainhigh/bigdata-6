#!/bin/bash



start=`date -d last-day +"%Y-%m-%d"`
end=`date -d now +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`
end_timestamp=`date -d "$end 00:00:00" +%s`

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
            set hive.optimize.sort.dynamic.partition=true;
            set parquet.compression=SNAPPY;

            DROP TABLE IF EXISTS tmp.mobp2p_borrow_tmp;
            create table tmp.mobp2p_borrow_tmp as
            select borrow_nid,user_id,addtime,borrow_success_time,IF(borrow_type = 7, 1, IF(borrow_type =6, 0, IF(borrow_type in (11,12),5,IF(borrow_type=8,4,99)))) borrow_type,borrow_period,status,account,repay_account_interest,app_version,add_channel,front_end_fee_flag from mobp2p.yyd_borrow;
            insert into tmp.mobp2p_borrow_tmp
            select borrow_nid,user_id,addtime,borrow_success_time,IF(borrow_period = 1, 3, IF(borrow_period = 8,6,2)) borrow_type,borrow_period,status,account,repay_account_interest,app_version,add_channel,front_end_fee_flag from mobp2p.yyd_u_borrow;

            insert overwrite table data_center.Report_Apply
            select
            b.borrow_nid borrow_id,
            a.user_id user_id,
            a.addtime Apply_Time,
            a.borrow_success_time Borrow_Time,
            a.borrow_type Product,
            a.borrow_period*30 borrow_period,
            a.status result,
            a.account amount,
            a.repay_account_interest Interest,
            a.app_version Version,
            a.add_channel Channel,
            b.verifier_type System_Verify
            from mobp2p.yyd_order_info b
            left join
            tmp.mobp2p_borrow_tmp a
            on b.borrow_nid=a.borrow_nid;


            insert overwrite table data_center.Report_Repay
            select
            b.id Schedule_ID,
            a.BORROW_NID BORROW_ID,
            a.USER_ID USER_ID,
            b.REPAY_PERIOD REPAY_PERIOD,
            a.BORROW_SUCCESS_TIME Borrow_Time,
            b.REPAY_TIME Plan_Repay_Time,
            b.REPAY_YESTIME Real_Repay_Time,
            IF(b.PRODUCT_TYPE='shoujidai', IF(b.BORROW_TYPE='MICROLOANS', 1, IF(b.BORROW_TYPE='NORMAL',0,4)), IF(b.PRODUCT_TYPE='yhfq',5,IF(b.PRODUCT_TYPE='uzone' AND b.BORROW_TYPE='PERIODLOAN',6,3))) Product,
            a.borrow_period borrow_period,
            IF(b.product_TYPE = 'shoujidai' AND b.BORROW_TYPE = 'PERIODLOAN', b.REPAY_ACCOUNT, b.REPAY_CAPITAL) Amount,
            b.REPAY_INTEREST Interest,
            b.repay_accrual Accrual,
            b.LATE_INTEREST LATE_INTEREST,
            b.Late_Reminder Late_Reminder,
            b.EXEMPTION_AMOUNT EXEMPTION_AMOUNT,
            a.front_end_fee_flag front_end_fee_flag
            from
            tmp.mobp2p_borrow_tmp a
            left join
            cw_mobp2p.t_repay_schedule b
            on a.borrow_nid=b.borrow_nid;


            DROP TABLE IF EXISTS tmp.yyd_rca_calc_amount_tmp;
            create table tmp.yyd_rca_calc_amount_tmp as
            select a.user_id,max(a.amount) as amount from mobp2p.yyd_rca_calc_amount a group by a.user_id;

            DROP TABLE IF EXISTS tmp.yyd_approve_creditcard_tmp;
            create table tmp.yyd_approve_creditcard_tmp as
            select a.user_id,max(a.ystatus) as ystatus from mobp2p.yyd_approve_creditcard a group by a.user_id;

            DROP TABLE IF EXISTS tmp.yyd_juxinli_gathdata_tmp;
            create table tmp.yyd_juxinli_gathdata_tmp as
            select a.user_id,concat_ws(' ',collect_set(a.website)) as website from mobp2p.yyd_juxinli_gathdata a group by a.user_id;

            insert overwrite table data_center.Report_Register PARTITION(cal_dt)
            select
            a.user_id user_id,
            IF(a.add_product = 'uzone', 1, IF(a.add_product='yinghuafenqi' OR a.add_channel = 'vip',2,0)) Product,
            a.reg_from reg_from,
            a.Reg_Time Reg_Time,
            IF(a.add_channel = '' OR a.add_channel IS NULL, a.add_channel, IF(a.add_channel='appstore' AND a.add_channel LIKE 'app_%', a.add_channel, yui.download_channel)) Channel,
            IF(PMOD(SUBSTRING(regexp_replace(b.card_id, 'test_',''),17,1),2)=0,'female','male') Sex,
            SUBSTRING(regexp_replace(b.card_id,'test_',''),7,8) Birthday,
            c.office Job,
            c.income Job_Income,
            c.address_province Job_Province,
            d.education_degree Degree,
            e.amount calc_amount,
            f.addtime ID_Time,
            f.card_id_status ID_status,
            g.contact_update_time Contact_Update_Time,
            IF(locate('jingdong',i.website)>0,1,0) Jingdong,
            IF(locate('mobile',i.website)>0 OR locate('chinatele',i.website)>0,1,0) Operator,
            h.ystatus CreditBanking,
            substr(from_unixtime(bigint(a.Reg_Time)),0,10) cal_dt
            from mobp2p.yyd_users a
            left join mobp2p.yyd_users_info yui
            on a.user_id=yui.user_id
            left join mobp2p.yyd_approve_realname b
            on a.user_id=b.user_id
            left join mobp2p.yyd_approve_job c
            on a.user_id=c.user_id
            left join mobp2p.yyd_edu_info d
            on a.user_id=d.user_id
            left join tmp.yyd_rca_calc_amount_tmp e
            on a.user_id=e.user_id
            left join mobp2p.yyd_approve_realname f
            on a.user_id=f.user_id
            left join mobp2p.yyd_v_reg_info g
            on a.user_id=g.user_id
            left join tmp.yyd_approve_creditcard_tmp h
            on a.user_id=h.user_id
            left join tmp.yyd_juxinli_gathdata_tmp i
            on a.user_id=i.user_id
            where a.reg_time>=${start_timestamp} and a.reg_time<${end_timestamp};


            insert overwrite table data_center.Report_Download
            select
            device_id device_id,
            IF(add_product='uzone',1,0) product,
            first_act_time create_time,
            first_down_chnl channel,
            first_version version
            from mis.mis_product_device;"
