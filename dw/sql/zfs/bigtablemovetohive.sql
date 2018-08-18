



CREATE TABLE `t_trustutn_log`(
`id` string,
`merchant_serial` string,
`transerials_id` string,
`channel` tinyint,
`service_type` string,
`request_status` tinyint,
`fee_flag` tinyint,
`request_text` string,
`response_text` string,
`create_time` string,
`create_user` string,
`update_time` string,
`update_user` string,
`warn_flag` tinyint)
COMMENT 't_trustutn_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

alter table t_trustutn_log rename to t_trustutn_log1



set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table auth_mobp2p.t_trustutn_log PARTITION(dt)
select *,substr(create_time,0,10) as dt
from auth_mobp2p.t_trustutn_log1;


select * from t_trustutn_log limit 100;


alter table t_tongdun_log rename to t_tongdun_log1;
CREATE TABLE `t_tongdun_log`(
`id` string,
`merchant_serial` string,
`transerials_id` string,
`channel` tinyint,
`service_type` string,
`request_status` tinyint,
`fee_flag` tinyint,
`request_text` string,
`response_text` string,
`create_time` string,
`create_user` string,
`update_time` string,
`update_user` string,
`warn_flag` tinyint)
COMMENT 't_tongdun_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;


set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table auth_mobp2p.t_tongdun_log PARTITION(dt)
select *,substr(create_time,0,10) as dt
from auth_mobp2p.t_tongdun_log1;

alter table t_kuaicha_log rename to t_kuaicha_log1;
CREATE TABLE `t_kuaicha_log`(
`id` string,
`merchant_serial` string,
`transerials_id` string,
`channel` tinyint,
`service_type` string,
`request_status` tinyint,
`fee_flag` tinyint,
`request_text` string,
`response_text` string,
`create_time` string,
`create_user` string,
`update_time` string,
`update_user` string,
`warn_flag` tinyint)
COMMENT 't_kuaicha_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table auth_mobp2p.t_kuaicha_log PARTITION(dt)
select *,substr(create_time,0,10) as dt
from auth_mobp2p.t_kuaicha_log1;

alter table t_zhihui_log rename to t_zhihui_log1;
CREATE TABLE `t_zhihui_log`(
`id` string,
`merchant_serial` string,
`transerials_id` string,
`channel` tinyint,
`service_type` string,
`request_status` tinyint,
`fee_flag` tinyint,
`request_text` string,
`response_text` string,
`create_time` string,
`create_user` string,
`update_time` string,
`update_user` string,
`warn_flag` tinyint)
COMMENT 't_zhihui_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table auth_mobp2p.t_zhihui_log PARTITION(dt)
select *,substr(create_time,0,10) as dt
from auth_mobp2p.t_zhihui_log1;


alter table t_credit100_log rename to t_credit100_log1;
CREATE TABLE `t_credit100_log`(
`id` string,
`merchant_serial` string,
`transerials_id` string,
`channel` tinyint,
`service_type` string,
`request_status` tinyint,
`fee_flag` tinyint,
`request_text` string,
`response_text` string,
`create_time` string,
`create_user` string,
`update_time` string,
`update_user` string,
`warn_flag` tinyint)
COMMENT 't_credit100_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert overwrite table auth_mobp2p.t_credit100_log PARTITION(dt)
select *,substr(create_time,0,10) as dt
from auth_mobp2p.t_credit100_log1;



alter table t_zhihui_log rename to t_zhihui_log1;
CREATE TABLE mongo.pq_customer_alter_log(
userid string,
datatype string,
datalog string,
addtime bigint,
addproduct string,
addchannel string,
col_name string,
col_value string)
COMMENT 'pq_customer_alter_log'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;

drop table mongo.customer_alter_log_tmp;
CREATE TABLE mongo.customer_alter_log_tmp(
userid string,
datatype string,
datalog string,
addtime bigint,
addproduct string,
addchannel string,
col_name string,
col_value string)
COMMENT 'customer_alter_log_tmp'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';