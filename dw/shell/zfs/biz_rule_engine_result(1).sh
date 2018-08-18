#!/bin/bash
#时间戳
logfile="/data2/gsj/log/$1.log"

start=`date -d last-day +"%Y-%m-%d"`

yr=`date -d last-day +"%Y"`
mn=`date -d last-day +"%m"`
dt=`date -d last-day +"%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl
su hdfs -c "spark-submit --class spark_hive_$1 --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/ql_etl_$1.jar"
echo "`date "+%Y-%m-%d %H:%M:%S"`  spark etl has done" >> $logfile


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
insert OVERWRITE table es_rule.$1 PARTITION(producttype,yr,mn,dt)
select taskId,taskTime,appName,appRequestId,inputParam,outputParam,ruleVersion,producttype as producttype,substr(from_unixtime(bigint($start_timestamp)),0,4) as yr,substr(from_unixtime(bigint($start_timestamp)),6,2) as mn,substr(from_unixtime(bigint($start_timestamp)),9,2) as dt from es_rule.$1_tmp;"


sudo -u hive hive -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table mysql_db.jg_ascore_v1_input_output PARTITION(yr,mn,dt)
SELECT
split(appRequestId,'_')[0] user_id,
split(appRequestId,'_')[1] borrow_nid,
taskTime as tasktime,
get_json_object(inputparam,'$.F0001') F0001,
get_json_object(inputparam,'$.F0002') F0002,
get_json_object(inputparam,'$.F0003') F0003,
get_json_object(inputparam,'$.F0004') F0004,
get_json_object(inputparam,'$.F0005') F0005,
get_json_object(inputparam,'$.F0006') F0006,
get_json_object(inputparam,'$.F0007') F0007,
get_json_object(inputparam,'$.F0008') F0008,
get_json_object(inputparam,'$.F0009') F0009,
get_json_object(inputparam,'$.F0010') F0010,
get_json_object(inputparam,'$.F0011') F0011,
get_json_object(inputparam,'$.F0012') F0012,
get_json_object(inputparam,'$.F0013') F0013,
get_json_object(inputparam,'$.F0014') F0014,
get_json_object(inputparam,'$.F0015') F0015,
get_json_object(inputparam,'$.F0016') F0016,
get_json_object(inputparam,'$.F0017') F0017,
get_json_object(inputparam,'$.F0018') F0018,
get_json_object(inputparam,'$.F0019') F0019,
get_json_object(inputparam,'$.F0020') F0020,
get_json_object(inputparam,'$.F0021') F0021,
get_json_object(inputparam,'$.F0022') F0022,
get_json_object(inputparam,'$.F0023') F0023,
get_json_object(inputparam,'$.F0024') F0024,
get_json_object(inputparam,'$.F0025') F0025,
get_json_object(inputparam,'$.F0026') F0026,
get_json_object(inputparam,'$.F0027') F0027,
get_json_object(inputparam,'$.F0028') F0028,
get_json_object(inputparam,'$.F0029') F0029,
get_json_object(inputparam,'$.F0030') F0030,
get_json_object(inputparam,'$.F0031') F0031,
get_json_object(inputparam,'$.F0032') F0032,
get_json_object(inputparam,'$.F0033') F0033,
get_json_object(inputparam,'$.F0034') F0034,
get_json_object(inputparam,'$.F0035') F0035,
get_json_object(inputparam,'$.F0036') F0036,
get_json_object(inputparam,'$.F0037') F0037,
get_json_object(inputparam,'$.F0038') F0038,
get_json_object(inputparam,'$.F0039') F0039,
get_json_object(outputparam,'$.model_total_score') model_total_score,
get_json_object(outputparam,'$.model_detail_score[0]') output01,
get_json_object(outputparam,'$.model_detail_score[1]') output02,
get_json_object(outputparam,'$.model_detail_score[2]') output03,
get_json_object(outputparam,'$.model_detail_score[3]') output04,
get_json_object(outputparam,'$.model_detail_score[4]') output05,
get_json_object(outputparam,'$.model_detail_score[5]') output06,
get_json_object(outputparam,'$.model_detail_score[6]') output07,
get_json_object(outputparam,'$.model_detail_score[7]') output08,
get_json_object(outputparam,'$.model_detail_score[8]') output09,
get_json_object(outputparam,'$.model_detail_score[9]') output10,
get_json_object(outputparam,'$.model_detail_score[10]') output11,
get_json_object(outputparam,'$.model_detail_score[11]') output12,
get_json_object(outputparam,'$.model_detail_score[12]') output13,
get_json_object(outputparam,'$.model_detail_score[13]') output14,
get_json_object(outputparam,'$.model_detail_score[14]') output15,
get_json_object(outputparam,'$.model_detail_score[15]') output16,
get_json_object(outputparam,'$.model_detail_score[16]') output17,
get_json_object(outputparam,'$.model_detail_score[17]') output18,
get_json_object(outputparam,'$.model_detail_score[18]') output19,
get_json_object(outputparam,'$.model_detail_score[19]') output20,
get_json_object(outputparam,'$.model_detail_score[20]') output21,
get_json_object(outputparam,'$.model_detail_score[21]') output22,
get_json_object(outputparam,'$.model_detail_score[22]') output23,
get_json_object(outputparam,'$.model_detail_score[23]') output24,
get_json_object(outputparam,'$.model_detail_score[24]') output25,
get_json_object(outputparam,'$.model_detail_score[25]') output26,
get_json_object(outputparam,'$.model_detail_score[26]') output27,
get_json_object(outputparam,'$.model_detail_score[27]') output28,
get_json_object(outputparam,'$.model_detail_score[28]') output29,
get_json_object(outputparam,'$.model_detail_score[29]') output30,
yr as yr,
mn as mn,
dt as dt
FROM
es_rule.biz_rule_engine_result where producttype='JG_Ascore_V1' and yr='$yr' and mn='$mn' and dt='$dt';
"

echo "`date "+%Y-%m-%d %H:%M:%S"`  $1 To hive has done" >> $logfile



drop table if exists rca.yyd_rca_sjd_zhaohui;
create table  rca.yyd_rca_sjd_zhaohui as
SELECT
split(appRequestId,'_')[0] user_id,
split(appRequestId,'_')[1] borrow_nid,
taskTime as tasktime,
get_json_object(inputparam,'$.same_job_address_cnt_30d') same_job_address_cnt_30d,
get_json_object(inputparam,'$.same_job_address_ip_cnt_30d') same_job_address_ip_cnt_30d,
get_json_object(inputparam,'$.same_phone8_cnt_30d') same_phone8_cnt_30d,
get_json_object(inputparam,'$.same_device_job_name_cnt_90d') same_device_job_name_cnt_90d,
get_json_object(inputparam,'$.same_device_job_address_cnt_90d') same_device_job_address_cnt_90d,
get_json_object(inputparam,'$.same_device_ip7_cnt_90d') same_device_ip7_cnt_90d,
get_json_object(inputparam,'$.same_job_tel_f_rate_90d') same_job_tel_f_rate_90d,
get_json_object(inputparam,'$.same_job_tel_f_cnt_90d') same_job_tel_f_cnt_90d,
concat(yr,'-',mn,'-',dt) as dt
FROM
es_rule.biz_rule_engine_result where producttype='sjd_zhaohui' and same_job_address_cnt_30d is not null



create table rca.yyd_rca_sjd_zhaohui(user_id string,borrow_nid string,tasktime string,same_job_address_cnt_30d string,same_job_address_ip_cnt_30d string,same_phone8_cnt_30d string,same_device_job_name_cnt_90d string,same_device_job_address_cnt_90d string,same_device_ip7_cnt_90d string,same_job_tel_f_rate_90d string,same_job_tel_f_cnt_90d string)
COMMENT 'rca.yyd_rca_sjd_zhaohui'
PARTITIONED BY(dt STRING)
STORED AS PARQUET;


sudo -u hive hive -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;
set hive.exec.mode.local.auto.tasks.max=20;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set parquet.compression=SNAPPY;
insert OVERWRITE table rca.yyd_rca_sjd_zhaohui PARTITION(dt)
SELECT
split(appRequestId,'_')[0] user_id,
split(appRequestId,'_')[1] borrow_nid,
taskTime as tasktime,
get_json_object(inputparam,'$.same_job_address_cnt_30d') same_job_address_cnt_30d,
get_json_object(inputparam,'$.same_job_address_ip_cnt_30d') same_job_address_ip_cnt_30d,
get_json_object(inputparam,'$.same_phone8_cnt_30d') same_phone8_cnt_30d,
get_json_object(inputparam,'$.same_device_job_name_cnt_90d') same_device_job_name_cnt_90d,
get_json_object(inputparam,'$.same_device_job_address_cnt_90d') same_device_job_address_cnt_90d,
get_json_object(inputparam,'$.same_device_ip7_cnt_90d') same_device_ip7_cnt_90d,
get_json_object(inputparam,'$.same_job_tel_f_rate_90d') same_job_tel_f_rate_90d,
get_json_object(inputparam,'$.same_job_tel_f_cnt_90d') same_job_tel_f_cnt_90d,
concat(yr,'-',mn,'-',dt) as dt
FROM
es_rule.biz_rule_engine_result where producttype='sjd_zhaohui' and get_json_object(inputparam,'$.same_job_address_cnt_30d') is not null and yr='$yr' and mn='$mn' and dt='$dt'"