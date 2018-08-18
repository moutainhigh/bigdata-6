#!/bin/bash
#时间戳

tables="mobile"
years="2017-08-23 2017-08-24 2017-08-25 2017-08-26 2017-08-27"
for table in $tables
do
    for i in $years;
    do
        mark=${i//-/}
        start_timestamp=`date -d "$i 00:00:00" +%s`
        end_timestamp=$(($start_timestamp+86400))
        echo "$table begin export,start_timestamp:$start_timestamp  end_timestamp:$end_timestamp"
        /root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c $table  -q '{addtime:{$gte:'${start_timestamp}',$lt:'${end_timestamp}'}}'  -o /data/$table-$mark.json

        sudo -u hive hadoop fs -rm -r /datahouse/ods/mongo/$table/$table-$mark.json

        hdfs dfs -put /data/$table-$mark.json  /datahouse/ods/mongo/$table/

        result=`hdfs dfs -ls /datahouse/ods/mongo/$table | grep $mark`
        if [[ "$result" == *$mark* ]];then
            rm -rf /data/$table-$mark.json
        fi

        echo "`date "+%Y-%m-%d %H:%M:%S"`  collections:$table $i end export ==================================="

    done
done
