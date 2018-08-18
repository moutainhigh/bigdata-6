create table dw_qlml.ncontacts201706(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201705(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201704(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201703(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201702(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201701(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201601(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201602(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201603(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201604(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201605(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201606(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201607(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201608(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201609(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201610(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201611(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts201612(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts2016ago(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

create table dw_qlml.ncontacts_tmp(user_id string,addtime string,tel string,name string,querytime string,device_id string,device_id_type string,mac string,idfa string,imei string,deviceupdatetime string,count string,tel_clean string,phonetype string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';



sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201705/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201705/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201704/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201704/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201703/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201703/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201702/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201702/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201701/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201701/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201612/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201612/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201611/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201611/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201610/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201610/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201609/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201609/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201608/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201608/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201607/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201607/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201606/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201606/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201605/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201605/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201604/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201604/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201603/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201603/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201602/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201602/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-201601/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts201601/
sudo -u hive hadoop dfs -cp /datahouse/ods/mongo/ncontacts_deal/ncontacts-2016ago/part-00000.snappy /user/hive/warehouse/dw_qlml.db/ncontacts2016ago/