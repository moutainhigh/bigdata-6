# 1,biz_microsite_zhuoyi_album
create table mongo.pq_biz_microsite_zhuoyi_album(user_id string,num string,lbs_list string,imei string,imsi string,fromChannel string,ac string,record_time string)
COMMENT 'biz_microsite_zhuoyi_album parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_album_tmp(user_id string,num string,lbs_list string,imei string,imsi string,fromChannel string,ac string,record_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';



#2,biz_microsite_zhuoyi_appdefault
create table mongo.pq_biz_microsite_zhuoyi_appdefault(user_id string,apk_def_list string,imei string,imsi string,fromChannel string,ac string,record_time string)
COMMENT 'biz_microsite_zhuoyi_appdefault parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_appdefault_tmp(user_id string,apk_def_list string,imei string,imsi string,fromChannel string,ac string,record_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


#3,biz_microsite_zhuoyi_apphisinfo
create table mongo.pq_biz_microsite_zhuoyi_apphisinfo(user_id string,app_name string,apk string,op string,imei string,imsi string,fromChannel string,ac string,record_time string)
COMMENT 'biz_microsite_zhuoyi_apphisinfo parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_apphisinfo_tmp(user_id string,app_name string,apk string,op string,imei string,imsi string,fromChannel string,ac string,record_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


#4,biz_microsite_zhuoyi_appinfo
create table mongo.pq_biz_microsite_zhuoyi_appinfo(user_id string,app_name string,apk string,imei string,imsi string,fromChannel string,ac string,record_time string)
COMMENT 'biz_microsite_zhuoyi_appinfo parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_appinfo_tmp(user_id string,app_name string,apk string,imei string,imsi string,fromChannel string,ac string,record_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


#5,biz_microsite_zhuoyi_appuserinfo
create table mongo.pq_biz_microsite_zhuoyi_appuserinfo(user_id string,apk_use_list string,imei string,imsi string,fromChannel string,ac string,record_time string,duration string,lbs string)
COMMENT 'biz_microsite_zhuoyi_appuserinfo parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_appuserinfo_tmp(user_id string,apk_use_list string,imei string,imsi string,fromChannel string,ac string,record_time string,duration string,lbs string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';



#6,biz_microsite_zhuoyi_simplesms
create table mongo.pq_biz_microsite_zhuoyi_simplesms(user_id string,imei string,imsi string,fromChannel string,ac string,record_time string)
COMMENT 'biz_microsite_zhuoyi_simplesms parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(user_id)  INTO 32 BUCKETS
STORED AS PARQUET;



create table tmp.biz_microsite_zhuoyi_simplesms_tmp(user_id string,imei string,imsi string,fromChannel string,ac string,record_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


