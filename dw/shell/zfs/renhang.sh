#!/bin/bash
#时间戳
logfile="/data2/gsj/renhang/logs/$1.log"

start=`date -d last-day +"%Y-%m-%d"`

start_timestamp=`date -d "$start 00:00:00" +%s`

#spark etl
su hdfs -c "spark-submit --class spark_hive_renhang --jars /data2/gsj/mongo_2/jars/fastjson-1.2.35.jar --deploy-mode client --num-executors 1 --executor-memory 2g --executor-cores 1 --driver-memory 5g --master yarn /data2/gsj/renhang/jars/ql_etl.jar 1"


renhang_table_list="report_reportpersonalinfo report_creditrecord_summary report_creditrecord_detail report_publicrecord_summary report_publicrecord_detail report_queryecord_summary report_queryecord_detail report_extend report_risk report_risk_loanBalanceInfos report_structure_basic report_structure_general report_structure_assets report_structure_compensates report_structure_guarantees report_structure_credits report_structure_loans report_structure_taxs report_structure_judgments report_structure_enforcements report_structure_punishments report_structure_telecoms report_structure_traces"
for i in $renhang_table_list;
do
    beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "set hive.exec.parallel=true;
    set hive.exec.parallel.thread.number=32;
    set hive.exec.mode.local.auto=true;
    set hive.exec.mode.local.auto.inputbytes.max=50000000;
    set hive.exec.mode.local.auto.tasks.max=20;
    set mapreduce.reduce.memory.mb=4096;
    set hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.exec.max.dynamic.partitions=100000;
    SET hive.exec.max.dynamic.partitions.pernode=100000;
    set parquet.compression=SNAPPY;
    insert OVERWRITE table dw_db.dw_$i PARTITION(yr,mn,dt)
    select *,substr(messageId,0,4) as yr,substr(messageId,5,2) as mn,substr(messageId,7,2) as dt from tmp.${i}_tmp;"
done
