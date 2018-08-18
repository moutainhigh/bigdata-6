#!/bin/bash

#https://stackoverflow.com/questions/11574410/how-to-find-the-size-of-a-hdfs-file

HOSTNAME="10.15.149.236"
PORT="3306"
USERNAME="root"
PASSWORD="chengce243"

DBNAME="data_logs"
TABLENAME="topic_monitor"

topics="bigdata_msgmgt_mobanker-customer_userloginfo bigdata_rca_jsystem_message bigdata_sensor_mobanker-customer_fillcontacts bigdata_sensor_mobanker-microsite_borrowclickstats bigdata_sensor_mobanker-microsite_commonrecordevent biz-uzone-wechat-channel-subscribe biz_crawler_collect biz_crawler_query biz_crawler_query_new biz_crawler_query_success biz_crawler_query_success_new biz_microsite_grabcallrds biz_microsite_grabcontacts biz_microsite_grablbs biz_microsite_grabmobile biz_microsite_grabshareuserinfologs biz_microsite_grabsms biz_microsite_zhuoyi_appinfo biz_microsite_zhuoyi_simplesms mobanker_auth_biz_data mobanker_authadptation_biz_data msgmgt-msg-retry_save pbccrc_report rule_engine_preanalysis_result rule_engine_result star_auth_fushu_raw"

for t in $topics;
do
    echo "topic is===============:$t"
    monitor_date=`date -d last-day +"%Y-%m-%d"`
    id=`date -d now +"%Y%m%d%H%M%S"`"_${t}"
    if hadoop fs -test -d  /datahouse/ods/topic/$t/$monitor_date ;
    then
        echo "yeah /datahouse/ods/topic/${t}/${monitor_date} it's there "
        size=`hadoop fs -du -s /datahouse/ods/topic/$t/$monitor_date  | awk '{s+=$1} END {printf "%s", s}'`
        status="1"
    else
        echo  "No /datahouse/ods/topic/${t}/${monitor_date} its not there."
        size=0
        status="0"
    fi

    echo "size:$size"
    create_time=`date -d now +"%Y-%m-%d %H:%M:%S"`
    topic_insert_sql="insert into ${TABLENAME}(id,topic,monitor_date,size,create_time,status) values('${id}','${t}','${monitor_date}','${size}','${create_time}','${status}')"
    mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e  "${topic_insert_sql}"
done