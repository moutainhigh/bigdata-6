

create table rca.yyd_rca_fushu_accounts(userId string,createTime string,updateTime string,loginAccount string,loginType string,id string,type string,card string,bank string,holder string,status string,bill_day string,repay_day string,credit_limit string,usable_limit string)
COMMENT 'rca.yyd_rca_fushu_accounts parquet file'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

create table rca.yyd_rca_fushu_accounts_tmp(userId string,createTime string,updateTime string,loginAccount string,loginType string,id string,type string,card string,bank string,holder string,status string,bill_day string,repay_day string,credit_limit string,usable_limit string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

-- ================================================================2=======================================
create table rca.yyd_rca_fushu_bills(userId string,createTime string,updateTime string,loginAccount string,loginType string,id string,account_id string,begin_date string,end_date string,bill_date string,repay_date string,payment string,least_payment string)
COMMENT 'rca.yyd_rca_fushu_bills parquet file'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

create table rca.yyd_rca_fushu_bills_tmp(userId string,createTime string,updateTime string,loginAccount string,loginType string,id string,account_id string,begin_date string,end_date string,bill_date string,repay_date string,payment string,least_payment string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

-- ===================================================================3===========================================

create table rca.yyd_rca_fushu_flows(userId string,createTime string,updateTime string,loginAccount string,loginType string,account_id string,bill_id string,trade_time string,settle_time string,trade_amount string,trade_amount_rmb string,trade_currency string,settle_amount string,settle_amount_rmb string,settle_currency string,balance string,account_no string,trade_description string,trade_nation string,trade_place string,trade_channel string,oppesite_name string,oppesite_bank string,oppesite_account string,summary string,postscript string)
COMMENT 'rca.yyd_rca_fushu_flows parquet file'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

create table rca.yyd_rca_fushu_flows_tmp(userId string,createTime string,updateTime string,loginAccount string,loginType string,account_id string,bill_id string,trade_time string,settle_time string,trade_amount string,trade_amount_rmb string,trade_currency string,settle_amount string,settle_amount_rmb string,settle_currency string,balance string,account_no string,trade_description string,trade_nation string,trade_place string,trade_channel string,oppesite_name string,oppesite_bank string,oppesite_account string,summary string,postscript string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

-- =======================================================================身份证 银行卡识别==========
create table rca.yyd_rca_idcard_validity
(user_id string,
type string,
validity_original string,
issueAuthority_original string,
address_original string,
birthday_original string,
idNumber_original string,
name_original string,
people_original string,
sex_original string,
status string,
error string,
msg string,
transerialsId string,
ask_time string,
ask_return_time string)
COMMENT 'rca.yyd_rca_idcard_validity parquet file'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

create table rca.yyd_rca_idcard_validity_tmp
(user_id string,
type string,
validity_original string,
issueAuthority_original string,
address_original string,
birthday_original string,
idNumber_original string,
name_original string,
people_original string,
sex_original string,
status string,
error string,
msg string,
transerialsId string,
ask_time string,
ask_return_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

-------------------------------------

create table rca.yyd_rca_bankcard_validity
(user_id string,
cardNumber_original string,
validate_original string,
type_original string,
Issuer_original string,
status string,
error string,
msg string,
transerialsId string,
ask_time string,
ask_return_time string)
COMMENT 'rca.yyd_rca_bankcard_validity parquet file'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

create table rca.yyd_rca_bankcard_validity_tmp
(user_id string,
cardNumber_original string,
validate_original string,
type_original string,
Issuer_original string,
status string,
error string,
msg string,
transerialsId string,
ask_time string,
ask_return_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';