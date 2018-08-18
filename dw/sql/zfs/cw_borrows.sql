CREATE TABLE cw_mobp2p.t_borrow1(
id string, 
borrow_original_id string, 
customer_name string, 
phone string, 
user_id string, 
name string, 
status string, 
account double, 
borrow_nid string, 
borrow_period int, 
period_days int, 
borrow_apr int, 
apr_days int, 
trading_fee_apr double, 
trading_fee_fixed double, 
borrow_time string, 
borrow_success_time string, 
repay_account_all double, 
repay_account_interest double, 
repay_account_capital double, 
repay_account_yes double, 
repay_account_interest_yes double, 
repay_account_capital_yes double, 
repay_account_wait double, 
repay_account_interest_wait double, 
repay_account_capital_wait double, 
exemption_amount double, 
repay_last_time string, 
repay_next_time string, 
repay_next_account double, 
repay_times int, 
late_interest double, 
late_forfeit double, 
late_reminder double, 
reverify_user_id string, 
reverify_time string, 
reverify_remark string, 
sn string, 
debitcard_num string, 
bank_name string, 
tender_status int, 
effective int, 
reason_fall string, 
product_type string, 
borrow_type string, 
product_channel string, 
loan_channel string, 
loan_fee double, 
repay_channel string, 
fddays int, 
front_end_fee_flag string, 
is_test int, 
flowflag string, 
app_version string, 
create_time string, 
create_user string, 
update_time string, 
update_user string, 
late_days_update int, 
repay_account_accrual double, 
repay_account_accrual_yes double)
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

sudo -u hive  hive -e "
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table cw_mobp2p.t_borrow1 PARTITION(dt)
select *,substr(create_time,0,7) as dt
from cw_mobp2p.t_borrow_his;"



CREATE TABLE cw_mobp2p.t_borrow_tmp(
id string,
borrow_original_id string,
customer_name string,
phone string,
user_id string,
name string,
status string,
account double,
borrow_nid string,
borrow_period int,
period_days int,
borrow_apr int,
apr_days int,
trading_fee_apr double,
trading_fee_fixed double,
borrow_time string,
borrow_success_time string,
repay_account_all double,
repay_account_interest double,
repay_account_capital double,
repay_account_yes double,
repay_account_interest_yes double,
repay_account_capital_yes double,
repay_account_wait double,
repay_account_interest_wait double,
repay_account_capital_wait double,
exemption_amount double,
repay_last_time string,
repay_next_time string,
repay_next_account double,
repay_times int,
late_interest double,
late_forfeit double,
late_reminder double,
reverify_user_id string,
reverify_time string,
reverify_remark string,
sn string,
debitcard_num string,
bank_name string,
tender_status int,
effective int,
reason_fall string,
product_type string,
borrow_type string,
product_channel string,
loan_channel string,
loan_fee double,
repay_channel string,
fddays int,
front_end_fee_flag string,
is_test int,
flowflag string,
app_version string,
create_time string,
create_user string,
update_time string,
update_user string,
late_days_update int,
repay_account_accrual double,
repay_account_accrual_yes double,
dt STRING);



select * ,from cw_mobp2p.t_borrow_his limit 10;;




sudo -u hive  hive -e "
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert into table cw_mobp2p.t_borrow1 PARTITION(dt)
select *,substr(create_time,0,7) as dt
from cw_mobp2p.t_borrow;"

sudo -u hive  hive -e "
insert into cw_mobp2p.t_borrow
select * ,from cw_mobp2p.t_borrow_his;
insert into cw_mobp2p.t_borrow_df
select * from cw_mobp2p.t_borrow_df_his;
insert into cw_mobp2p.t_borrow_extend
select * from cw_mobp2p.t_borrow_extend_his;"