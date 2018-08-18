create table dw_qlml.ncallrecords201706(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201705(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201704(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201703(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201702(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201701(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201601(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201602(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201603(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201604(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201605(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201606(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201607(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201608(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201609(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201610(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201611(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords201612(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords2016ago(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncallrecords_tmp(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201705/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201704/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201704/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201703/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201703/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201702/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201702/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201701/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201701/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201612/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201612/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201611/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201611/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201610/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201610/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201609/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201609/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201608/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201608/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201607/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201607/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201606/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201606/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201605/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201605/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201604/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201604/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201603/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201603/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201602/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201602/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-201601/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords201601/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncallrecords_deal/ncallrecords-2016ago/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncallrecords2016ago/

#新增字段  这个添加时要特别注意验证
ALTER TABLE mongo.pq_ncallrecords ADD COLUMNS (imsi string,record_time string,ac string,fromChannel string);

create table dw_qlml.ncallrecords_tmp(user_id string,addtime string,count string,calltime string,type string,duration string,phone string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,imsi string,record_time string,ac string,fromChannel string,phone_clean string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';