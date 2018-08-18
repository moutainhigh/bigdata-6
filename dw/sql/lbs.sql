create table dw_qlml.lbs201706(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201705(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201704(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201703(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201702(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201701(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201601(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201602(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201603(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201604(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201605(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201606(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201607(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201608(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201609(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201610(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201611(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs201612(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs2016ago(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.lbs_tmp(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';




sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201705/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201704/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201704/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201703/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201703/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201702/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201702/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201701/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201701/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201612/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201612/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201611/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201611/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201610/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201610/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201609/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201609/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201608/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201608/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201607/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201607/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201606/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201606/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201605/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201605/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201604/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201604/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201603/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201603/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201602/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201602/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-201601/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs201601/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/lbs_deal/lbs-2016ago/part-00000.snappy /user/hive/warehouse/dw_qlml.db/lbs2016ago/

#新增字段
ALTER TABLE mongo.pq_lbs ADD COLUMNS (imsi string,ac string,lbs string,province string,city string,record_time string,fromChannel string);

create table dw_qlml.lbs_tmp(user_id string,openid string,addtime string,lat string,lon string,mapp string,type string,precision string,device_id string,device_id_type string,ip string,mac string,idfa string,imei string,add_product string,add_channel string,imsi string,ac string,lbs string,province string,city string,record_time string,fromChannel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';
