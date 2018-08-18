CREATE TABLE IF NOT EXISTS `topic_monitor`(
   `id` VARCHAR(100) NOT NULL , -- topic+yyyyMMddHHmmSS
   `topic` VARCHAR(100) ,
   `monitor_date` VARCHAR(100) ,
   `size` double,  -- 文件大小
   `create_time` datetime ,
   `status` VARCHAR(100) ,   -- 0:不存在，1:存在
   PRIMARY KEY ( `id` )
);