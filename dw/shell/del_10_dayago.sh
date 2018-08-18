#!/bin/bash
#时间戳
logfile="/data2/gsj/log/del_lichuanliang_table.log"

start=`date -d "-11 day" +"%Y-%m-%d"`

del_table_list="dw_yyd_users dw_t_repay_schedule dw_yyd_borrow dw_t_borrow_df dw_yyd_borrow_channel_full"
for i in $del_table_list;
do
beeline -u "jdbc:hive2://master2:10000/" -n gongshaojie -w /data2/gsj/beelinepwd.txt -e "
alter table dw_db.$i drop partition(cal_dt<='$start');
"
done
