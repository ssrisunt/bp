#!/bin/bash

# #1
# recent M days, top N domains
# (M >= 0) and (N >= 0)
time_start_1=$(date "+%s%3N")

##pig -f web.pig -param m=10000000 -param n=20 -param domain='.*' -param time_start=$time_start_1
time_end_1=$(date "+%s%3N")
#echo "time_start_1: $time_start_1"
#echo "time_end_1: $time_end_1"

node /home/weichi/hw1/thrift/command.js put RDBPHistory $time_start_1 cf BP1 $time_end_1


##printf "put 'bp_timestamp', 'bp1_start', 'Time', $time_start_1
##put 'bp_timestamp', 'bp1_end', 'Time', $time_end_1
##exit" > cmd.txt

##hbase shell cmd.txt
##rm cmd.txt

##printf "result:
##time_start_1: $time_start_1
##time_end_1: $time_end_1
##" >> bp_log.txt






