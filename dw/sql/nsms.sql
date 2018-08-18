create table dw_qlml.nsms201706(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


create table dw_qlml.nsms201705(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201704(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201703(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201702(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201701(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201601(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201602(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201603(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201604(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201605(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201606(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201607(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201608(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201609(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201610(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201611(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms201612(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms2016ago(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.nsms_tmp(user_id string,addtime string,calltime string,content string,type string,phone string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,name string,count string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201705/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201704/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201704/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201703/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201703/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201702/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201702/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201701/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201701/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201612/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201612/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201611/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201611/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201610/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201610/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201609/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201609/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201608/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201608/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201607/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201607/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201606/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201606/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201605/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201605/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201604/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201604/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201603/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201603/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201602/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201602/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-201601/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms201601/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/nsms_deal/nsms-2016ago/part-00000.snappy /user/hive/warehouse/dw_qlml.db/nsms2016ago/