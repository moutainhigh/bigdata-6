#dw_log_monitor
create table decision.dw_riskdata_log_monitor(user_id string,query_id string,query_string string,submit_time string,cost_time string)
COMMENT 'pq_dw_log_monitor parquet file'
PARTITIONED BY(yr STRING,mn STRING,dt STRING)
CLUSTERED BY(query_id)  INTO 32 BUCKETS
STORED AS PARQUET;

create table tmp.dw_log_monitor_tmp(user_id string,query_id string,query_string string,submit_time string,cost_time string,finishTime string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';