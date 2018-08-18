#!/bin/bash

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="data_sync_log"

ydum_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ydum_task_id=`date -d now +"%Y%m%d%H%M%S"`"_yyd_device_user_mapping"

sudo -u hive sqoop import \
--connect "jdbc:mysql://main.read.db.ql.corp:33944/mobp2p?useUnicode=true&characterEncoding=gbk" \
--username bigdataetluser \
--password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH \
--table yyd_device_user_mapping \
--split-by 'device_id' \
-m 30 \
--delete-target-dir \
--hive-overwrite \
--hive-import \
--hive-database mobp2p \
--hive-table yyd_device_user_mapping \
--null-string '\\N' \
--null-non-string '\\N' \

ydum_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ydum_start_timestamp=`date -d "$ydum_start_time" +%s`
ydum_end_timestamp=`date -d "$ydum_end_time" +%s`
ydum_cost_time=$(($ydum_end_timestamp-$ydum_start_timestamp))

ydum_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${ydum_task_id}','main.read.db.ql.corp','mobp2p','yyd_device_user_mapping','${ydum_start_time}','${ydum_end_time}','${ydum_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ydum_insert_sql}"


sudo -u hive hive dfs -rm -r /user/hive/t_baddebt_repay
sudo -u hive hive dfs -rm -r /user/hive/t_link_info
sudo -u hive hive dfs -rm -r /user/hive/t_plan_share_log
sudo -u hive hive dfs -rm -r /user/hive/t_repay_late_fee_log
sudo -u hive hive dfs -rm -r /user/hive/t_suanhua_spectrade_lib
sudo -u hive hive dfs -rm -r /user/hive/t_udecondynamic_lib
sudo -u hive hive dfs -rm -r /user/hive/t_zhima_auth_lib
sudo -u hive hive dfs -rm -r /user/hive/yyd_limit_human_amount_record
sudo -u hive hdfs dfs -rm -r /user/hive/yyd_pingan_black_list


trlfl_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
trlfl_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_repay_late_fee_log"

sudo -u hive sqoop import --connect "jdbc:mysql://cwpay.read.db.ql.corp:33944/cw_mobp2p?useUnicode=true" --table t_repay_late_fee_log --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database cw_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_repay_late_fee_log

trlfl_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
trlfl_start_timestamp=`date -d "$trlfl_start_time" +%s`
trlfl_end_timestamp=`date -d "$trlfl_end_time" +%s`
trlfl_cost_time=$(($trlfl_end_timestamp-$trlfl_start_timestamp))

trlfl_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${trlfl_task_id}','cwpay.read.db.ql.corp','cw_mobp2p','t_repay_late_fee_log','${trlfl_start_time}','${trlfl_end_time}','${trlfl_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${trlfl_insert_sql}"


#============================================================
ylhar_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ylhar_task_id=`date -d now +"%Y%m%d%H%M%S"`"_yyd_limit_human_amount_record"

sudo -u hive sqoop import --connect "jdbc:mysql://main.read.db.ql.corp:33944/mobp2p?useUnicode=true&characterEncoding=gbk" --query "select id, user_id, cast(borrow_nid as char(100)) as borrow_nid, borrow_amount, borrow_type, huma_desc, temp_amount, huma_amount, calc_amount, cred_amount, remark, product, addtime from yyd_limit_human_amount_record where \$CONDITIONS;"  --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-drop-import-delims --hive-table yyd_limit_human_amount_record --target-dir /user/hive/yyd_limit_human_amount_record -m 1

ylhar_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ylhar_start_timestamp=`date -d "$ylhar_start_time" +%s`
ylhar_end_timestamp=`date -d "$ylhar_end_time" +%s`
ylhar_cost_time=$(($ylhar_end_timestamp-$ylhar_start_timestamp))

ylhar_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${ylhar_task_id}','main.read.db.ql.corp','mobp2p','yyd_limit_human_amount_record','${ylhar_start_time}','${ylhar_end_time}','${ylhar_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ylhar_insert_sql}"

#==================================
tbr_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tbr_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_baddebt_repay"

sudo -u hive sqoop import --connect "jdbc:mysql://cwpay.read.db.ql.corp:33944/cw_mobp2p?useUnicode=true" --table t_baddebt_repay --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database cw_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_baddebt_repay

tbr_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tbr_start_timestamp=`date -d "$tbr_start_time" +%s`
tbr_end_timestamp=`date -d "$tbr_end_time" +%s`
tbr_cost_time=$(($tbr_end_timestamp-$tbr_start_timestamp))

tbr_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tbr_task_id}','cwpay.read.db.ql.corp','cw_mobp2p','t_baddebt_repay','${tbr_start_time}','${tbr_end_time}','${tbr_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tbr_insert_sql}"

#======================================

tli_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tli_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_link_info"

sudo -u hive sqoop import --connect "jdbc:mysql://consumer.read.db.ql.corp:33944/consumer_mobp2p?useUnicode=true" --query "select id, create_user, create_time, update_user, update_time, phone_num, type, linkman_name, linkman_relation, linkman_phone,state,cast(remark as char(400)) from t_link_info where \$CONDITIONS;"  --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database consumer_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-drop-import-delims --hive-table t_link_info --target-dir /user/hive/t_link_info  -m 1

tli_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tli_start_timestamp=`date -d "$tli_start_time" +%s`
tli_end_timestamp=`date -d "$tli_end_time" +%s`
tli_cost_time=$(($tli_end_timestamp-$tli_start_timestamp))

tli_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tli_task_id}','consumer.read.db.ql.corp','consumer_mobp2p','t_link_info','${tli_start_time}','${tli_end_time}','${tli_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tli_insert_sql}"

#======================================
tpsl_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tpsl_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_plan_share_log"

sudo -u hive sqoop import --connect "jdbc:mysql://activity.read.db.ql.corp:33944/activity_mobp2p?useUnicode=true" --table t_plan_share_log --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database activity_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_plan_share_log   

tpsl_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tpsl_start_timestamp=`date -d "$tpsl_start_time" +%s`
tpsl_end_timestamp=`date -d "$tpsl_end_time" +%s`
tpsl_cost_time=$(($tpsl_end_timestamp-$tpsl_start_timestamp))

tpsl_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tpsl_task_id}','activity.read.db.ql.corp','activity_mobp2p','t_plan_share_log','${tpsl_start_time}','${tpsl_end_time}','${tpsl_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tpsl_insert_sql}"

#======================================
tssl_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tssl_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_suanhua_spectrade_lib"

sudo -u hive sqoop import --connect "jdbc:mysql://auth.read.db.ql.corp:33944/auth_mobp2p?useUnicode=true" --table t_suanhua_spectrade_lib --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database auth_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_suanhua_spectrade_lib 

tssl_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tssl_start_timestamp=`date -d "$tssl_start_time" +%s`
tssl_end_timestamp=`date -d "$tssl_end_time" +%s`
tssl_cost_time=$(($tssl_end_timestamp-$tssl_start_timestamp))

tssl_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tssl_task_id}','auth.read.db.ql.corp','auth_mobp2p','t_suanhua_spectrade_lib','${tssl_start_time}','${tssl_end_time}','${tssl_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tssl_insert_sql}"

#======================================
tul_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tul_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_udecondynamic_lib"

sudo -u hive sqoop import --connect "jdbc:mysql://auth.read.db.ql.corp:33944/auth_mobp2p?useUnicode=true&characterEncoding=gbk" --table t_udecondynamic_lib --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database auth_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_udecondynamic_lib 

tul_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tul_start_timestamp=`date -d "$tul_start_time" +%s`
tul_end_timestamp=`date -d "$tul_end_time" +%s`
tul_cost_time=$(($tul_end_timestamp-$tul_start_timestamp))

tul_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tul_task_id}','auth.read.db.ql.corp','auth_mobp2p','t_udecondynamic_lib','${tul_start_time}','${tul_end_time}','${tul_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tul_insert_sql}"

#======================================
tzal_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tzal_task_id=`date -d now +"%Y%m%d%H%M%S"`"_t_zhima_auth_lib"

sudo -u hive sqoop import --connect "jdbc:mysql://auth.read.db.ql.corp:33944/auth_mobp2p?useUnicode=true" --table t_zhima_auth_lib --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database auth_mobp2p --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table t_zhima_auth_lib  

tzal_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
tzal_start_timestamp=`date -d "$tzal_start_time" +%s`
tzal_end_timestamp=`date -d "$tzal_end_time" +%s`
tzal_cost_time=$(($tzal_end_timestamp-$tzal_start_timestamp))

tzal_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${tzal_task_id}','auth.read.db.ql.corp','auth_mobp2p','t_zhima_auth_lib','${tzal_start_time}','${tzal_end_time}','${tzal_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${tzal_insert_sql}"

#======================================
ypbl_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ypbl_task_id=`date -d now +"%Y%m%d%H%M%S"`"_yyd_pingan_black_list"

sudo -u hive sqoop import --connect "jdbc:mysql://rca.read.db.ql.corp:33944/rca?useUnicode=true&characterEncoding=gbk" --table yyd_pingan_black_list --username bigdataetluser --password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH --hive-import --hive-database rca --hive-overwrite --null-non-string '\\N' --null-string '\\N' --hive-drop-import-delims --hive-table yyd_pingan_black_list  -m 1

ypbl_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ypbl_start_timestamp=`date -d "$ypbl_start_time" +%s`
ypbl_end_timestamp=`date -d "$ypbl_end_time" +%s`
ypbl_cost_time=$(($ypbl_end_timestamp-$ypbl_start_timestamp))

ypbl_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${ypbl_task_id}','rca.read.db.ql.corp','rca','yyd_pingan_black_list','${ypbl_start_time}','${ypbl_end_time}','${ypbl_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ypbl_insert_sql}"

batch_date=`date "+%Y-%m-%d" -d "-1day"` 

#从mysql向多分区hive表中导入数据
ycir_start_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ycir_task_id=`date -d now +"%Y%m%d%H%M%S"`"_yyd_credit_interface_result"

sudo -u hdfs sqoop import \
--connect jdbc:mysql://rca.read.db.ql.corp:33944/rca \
--username bigdataetluser \
--password tmTvi8Fn9EjlBD8zT6SN9pk8K4o7RH \
--table  yyd_credit_interface_result  \
--where "qry_time>=UNIX_TIMESTAMP('$batch_date') AND qry_time <UNIX_TIMESTAMP(DATE_SUB('$batch_date',INTERVAL -1 DAY))" \
--delete-target-dir \
--hive-overwrite \
--hive-import \
--hive-database rca \
--hive-table yyd_credit_interface_result \
--hive-partition-key   dt   \
--hive-partition-value  $batch_date \
--null-string '\\N' \
--null-non-string '\\N' \
--hive-drop-import-delims

ycir_end_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
ycir_start_timestamp=`date -d "$ycir_start_time" +%s`
ycir_end_timestamp=`date -d "$ycir_end_time" +%s`
ycir_cost_time=$(($ycir_end_timestamp-$ycir_start_timestamp))

ycir_insert_sql="insert into ${TABLENAME}(task_id,service_ip,db_name,table_name,start_time,end_time,cost_time,status) values('${ycir_task_id}','rca.read.db.ql.corp','rca','yyd_credit_interface_result','${ycir_start_time}','${ycir_end_time}','${ycir_cost_time}','1')"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${ycir_insert_sql}"


