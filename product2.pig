--register /usr/lib/hbase/lib/*.jar;

%declare stime $time_start

/**/

register /usr/hdp/2.2.0.0-2041/pig/piggybank.jar;
define Stitch org.apache.pig.piggybank.evaluation.Stitch;
define Over org.apache.pig.piggybank.evaluation.Over('int');

/*
A = load '/user/hdfs/wc_test2/part-r-00000_v2' using PigStorage('\u0001')
as (rowid:int, user_id:chararray, domain_name:chararray,
url:chararray, ip_address:chararray, dump_time:chararray,
referer:chararray, session_id:chararray,
ecid:chararray, product_id:chararray);
describe A;
*/

A = load '/user/hdfs/ReddoorExportDb' using PigStorage('\u0001')
as (log_id:int, user_id:chararray, domain_name:chararray,
url:chararray, ip_address:chararray, dump_time:chararray,
referer:chararray, session_id:chararray, page_title:chararray,
sex:chararray, age:int, live_city:chararray,
ecid:chararray, product_id:chararray,
eccatalog_name:chararray, pgcatalog_name:chararray,
name:chararray, price:chararray, object_name:chararray, brand:chararray);
describe A;

A2 = foreach A generate user_id, dump_time, object_name,
                        DaysBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as date;

A3 = filter A2 by (date >= 0) and (date < $m) and (object_name is not null)
     and (not object_name matches 'null');

--A32 = filter A3 by user_id == '0007EAB6-0EAF-4FE9-96AE-145DAC62C8CE';
--A32 = filter A3 by user_id == '00044A91-6C92-4145-A8AF-D53D4CA494A2';

A33 = foreach A3 generate user_id, dump_time, date, flatten(STRSPLIT(object_name, '\\|')); -- {i}
A331 = foreach A33 generate (chararray)$0, (chararray)$1, (long)$2, (bag{tuple()})TOBAG($3 ..) as bbb:bag{ttt:tuple()};
--A331 = foreach A33 generate (chararray)$0, (chararray)$1, (long)$2, (bag{tuple(chararray)})TOBAG($3 ..);

--A34 = foreach A33 generate user_id, dump_time, date, flatten(TOBAG($3 ..)) as object_single;
--A34 = foreach A331 generate $0 .. $2, flatten(TOBAG($3 ..));
A34 = foreach A331 generate $0 .. $2, flatten($3);

A341 = foreach A34 generate (chararray)$0 as user_id, (chararray)$1, (long)$2, (chararray)$3 as object_name;

A4 = group A341 by (user_id, object_name);

A5 = foreach A4 generate group.user_id as user_id,
                         group.object_name as object_name,
                         COUNT(A341) as object_count;

B = group A5 by user_id;
describe B;

C = foreach B {
  C1 = order A5 by object_count desc, object_name asc;
  generate flatten(Stitch(Over(C1, 'row_number'), C1));
};
describe C;

D = foreach C generate stitched::user_id as user_id,
                       stitched::object_name as object_name,
                       stitched::object_count as object_count,
                       $0 as row_id,
                       CONCAT(CONCAT(CONCAT(object_name, ' ('), (chararray)object_count), ')') as object_with_count;
                       --CONCAT((bytearray)'test----', object_name);
describe D;

D2 = filter D by (row_id <= $n);
describe D2;

E = group D2 by user_id;
E2 = foreach E {
  EE = order D2 by row_id asc;
--  generate group as key, BagToString(EE.domain_name, ',') as value, $time_start;
  generate CONCAT($0, CONCAT('_',(chararray)'$stime')) as key, BagToString(EE.object_with_count, ',') as value;
};
describe E2;

--store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
--  'BatchProcessResult:BP1 BatchProcessResult:timestamp');

store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
  'BatchProcessResult:BP4');





