# !/bin/bash

set -x

curday=`date +'%Y-%m-%d'`
curdate=`date +'%Y-%m-%d %H:%M:%S'`
cbegin=`date +'%Y-%m-%d %H:%M:%S'`
lastday=`date -d "$curdate 1 day ago" +'%Y-%m-%d'`
# 如果传入执行id则直接使用否则生成
taskid=`echo $curdate$*|md5sum|tr -d " -"`  # 根据时间及参数生成调用id


biztype=juxinClean
deploydir=/data/shazh
wathclog=$deploydir/cw_clean.log

echo `date +'%Y-%m-%d %H:%M:%S'`" -- INFO -- ""taskid:$taskid,callfrom:$0,pid:$$,stage:0/5,parameters:$*" >> $wathclog
# 输入参数:开始日期  结束日期  运行模式(1-清洗, 2-规格化处理, 3-load到hive)
# 开始日期,结束日期,表名(juxinli_auth_log)
if [ x$1 = xyesterday ]  || [ x$1 = x ];then
    begin=$lastday
    end=$curday
    model=$2 #   
else
    begin=$1
    end=$2
    model=123
fi


if [ x$3 != x ];then
    model=$3
fi

# 日期循环
daylen=$(( ($(date -d "$end" +%s) - $(date -d "$begin" +%s))/(24*60*60) ))  # 起始结束日期之间间隔天数
pdays=${daylen#-}


#tablelist=${!schmap[@]}

# 清洗原始数据
if [[ "$model" == *1* ]];then
    echo `date +'%Y-%m-%d %H:%M:%S'`" -- INFO -- ""taskid:$taskid,1.清洗:JuxinliAuthLogClean" >> $wathclog
          #### 1.0 执行计算
    sudo -u hdfs spark-submit --class com.mobanker.hadoop.Json2Column \
     --master yarn  \
     --deploy-mode client \
     --driver-memory 1g \
     --executor-memory 2g  \
     --num-executors 4 \
    /data/shazh/bigdata-1.0-SNAPSHOT.jar $begin $end messageId,timestamp,typeId,data.channel,data.code,data.error,data.msg,data.status,data.transerialsId,properties.product,properties.system,properties.$provider,properties.transerialsId,properties.userId,properties.productType  /datahouse/ods/topic/mobanker_authadptation_biz_data  /datahouse/ods/clean/mobanker_authadptation_biz_data

fi

sudo -u hdfs hdfs dfs -chmod -R 777 /datahouse/ods/clean


if [[ "$model" == *3* ]];then
    table=authadptation_biz_data
    #for i in `seq 0 $pdays`; 
    for ((i=0;i<=$pdays;i++));
      do 
        echo `date +'%Y-%m-%d %H:%M:%S'`" -- INFO -- ""taskid:$taskid,3.load到hive,table-${table},日期:${i}" >> $wathclog
        tagdate=`date -d "$begin $i day" +'%Y-%m-%d'`
        frompath=/datahouse/ods/clean/${table}/${tagdate}/part*
        #t_juxinli_contact_region_lib
        schclns=${schmap[${table}]}
        # DS层原始表写入
        addstr="alter table ds_db.ds_k_${table} add IF NOT EXISTS partition(cal_dt='${tagdate}');"
        writestr="LOAD DATA INPATH '$frompath' OVERWRITE INTO TABLE ds_db.ds_k_${table} partition(cal_dt='${tagdate}');"

        # 全局属性设置
        setstr="set parquet.compression=SNAPPY;"

        sudo -u hive hive -e "${addstr} ${writestr}"
      done
fi

# 时间统计
cend=`date +'%Y-%m-%d %H:%M:%S'`
secbegin=`date -d  "$cbegin" +%s`
secend=`date -d  "$cend" +%s`
timeuse=$(($secend-$secbegin))
echo `date +'%Y-%m-%d %H:%M:%S'`" -- INFO -- ""taskid:$taskid,biztype:$biztype,callfrom:$0,stage:4/4 end,begin:$cbegin,end:$cend,timeuse:$timeuse" >> $wathclog
