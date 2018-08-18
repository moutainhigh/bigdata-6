#!/bin/bash
filename="/data/lxy/userid.txt"
for  user  in  `cat $filename`
do
exho $user
/root/mongodb-linux-x86_64-2.4.10/bin/mongoexport -h 10.15.168.4 --port 37011 -d mobp2p -c ncallrecords  -q '{user_id:'${user}'}'  -o ./ncallrecords.json
done

