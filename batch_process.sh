#!/bin/bash

#while true
#do

: << 'END'
END

# #1
# recent M days, top N domains
# (M >= 0) and (N >= 0)
time_start_1=$(date "+%s%3N")
pig -f /home/weichi/reddoor/bp/web.pig -param m=10000000 -param n=20 -param time_start=$time_start_1
time_end_1=$(date "+%s%3N")
#echo "time_start_1: $time_start_1"
#echo "time_end_1: $time_end_1"

node /home/weichi/reddoor/bp/thrift/command.js put RDBPHistory $time_start_1 cf BP1 $time_end_1

printf "put 'bp_timestamp', 'bp1_start', 'Time', $time_start_1
put 'bp_timestamp', 'bp1_end', 'Time', $time_end_1
exit" > /home/weichi/reddoor/bp/cmd.txt

hbase shell /home/weichi/reddoor/bp/cmd.txt
rm /home/weichi/reddoor/bp/cmd.txt

# #2
# recent M days, top N domains, filter specific domains
time_start_2=$(date "+%s%3N")
#pig -f /home/weichi/reddoor/bp/web2.pig -param m=10000000 -param n=20 -param domain='.*yahoo.*' -param time_start=$time_start_2
pig -f /home/weichi/reddoor/bp/web2.pig -param m=10000000 -param n=20 -param time_start=$time_start_2
time_end_2=$(date "+%s%3N")
#echo "time_start_2: $time_start_2"
#echo "time_end_2: $time_end_2"

node /home/weichi/reddoor/bp/thrift/command.js put RDBPHistory $time_start_2 cf BP2 $time_end_2

printf "put 'bp_timestamp', 'bp2_start', 'Time', $time_start_2
put 'bp_timestamp', 'bp2_end', 'Time', $time_end_2
exit" > /home/weichi/hw1/cmd.txt

hbase shell /home/weichi/reddoor/bp/cmd.txt
rm /home/weichi/reddoor/bp/cmd.txt

# #3
# recent M days, top N products
time_start_3=$(date "+%s%3N")
pig -f /home/weichi/reddoor/bp/product.pig -param m=10000000 -param n=20 -param time_start=$time_start_3
time_end_3=$(date "+%s%3N")
#echo "time_start_3: $time_start_3"
#echo "time_end_3: $time_end_3"

node /home/weichi/reddoor/bp/thrift/command.js put RDBPHistory $time_start_3 cf BP3 $time_end_3

printf "put 'bp_timestamp', 'bp3_start', 'Time', $time_start_3
put 'bp_timestamp', 'bp3_end', 'Time', $time_end_3
exit" > /home/weichi/reddoor/bp/cmd.txt

hbase shell /home/weichi/reddoor/bp/cmd.txt
rm /home/weichi/reddoor/bp/cmd.txt

# #4
# recent M days, top N categories, order by frequency
time_start_4=$(date "+%s%3N")
pig -f /home/weichi/reddoor/bp/product2.pig -param m=10000000 -param n=20 -param time_start=$time_start_4
time_end_4=$(date "+%s%3N")
#echo "time_start_4: $time_start_4"
#echo "time_end_4: $time_end_4"

node /home/weichi/reddoor/bp/thrift/command.js put RDBPHistory $time_start_4 cf BP4 $time_end_4

printf "put 'bp_timestamp', 'bp4_start', 'Time', $time_start_4
put 'bp_timestamp', 'bp4_end', 'Time', $time_end_4
exit" > /home/weichi/reddoor/bp/cmd.txt

hbase shell /home/weichi/reddoor/bp/cmd.txt
rm /home/weichi/reddoor/bp/cmd.txt

# #5
# recent M days, top N categories, order by date
time_start_5=$(date "+%s%3N")
pig -f /home/weichi/reddoor/bp/product3.pig -param m=10000000 -param n=20 -param time_start=$time_start_5
time_end_5=$(date "+%s%3N")
#echo "time_start_5: $time_start_5"
#echo "time_end_5: $time_end_5"

node /home/weichi/reddoor/bp/thrift/command.js put RDBPHistory $time_start_5 cf BP5 $time_end_5

printf "put 'bp_timestamp', 'bp5_start', 'Time', $time_start_5
put 'bp_timestamp', 'bp5_end', 'Time', $time_end_5
exit" > /home/weichi/reddoor/bp/cmd.txt

hbase shell /home/weichi/reddoor/bp/cmd.txt
rm /home/weichi/reddoor/bp/cmd.txt

: << 'END'
END

printf "result:
time_start_1: $time_start_1
time_end_1: $time_end_1
time_start_2: $time_start_2
time_end_2: $time_end_2
time_start_3: $time_start_3
time_end_3: $time_end_3
time_start_4: $time_start_4
time_end_4: $time_end_4
time_start_5: $time_start_5
time_end_5: $time_end_5
" >> /home/weichi/reddoor/bp/bp_log.txt

#done




