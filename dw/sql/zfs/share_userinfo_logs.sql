create table dw_qlml.share_userinfo_logs201705(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201704(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201703(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201702(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201701(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201601(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201602(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201603(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201604(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201605(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201606(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201607(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201608(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201609(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201610(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201611(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs201612(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs2016ago(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.share_userinfo_logs_tmp(user_id string,addtime string,type string,add_ip string,productcode string,channel string,xin_info string,old_info string,status string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201705/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201704/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201704/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201703/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201703/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201702/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201702/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201701/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201701/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201612/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201612/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201611/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201611/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201610/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201610/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201609/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201609/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201608/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201608/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201607/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201607/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201606/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201606/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201605/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201605/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201604/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201604/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201603/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201603/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201602/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201602/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-201601/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs201601/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/share_userinfo_logs_deal/share_userinfo_logs-2016ago/part-00000.snappy /user/hive/warehouse/dw_qlml.db/share_userinfo_logs2016ago/
