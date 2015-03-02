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

A2 = foreach A generate user_id,dump_time,object_name,
                        DaysBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as date;

A3 = filter A2 by (date >= 0) and (date < $m) and (object_name is not null)
     and (not object_name matches 'null');

A4 = group A3 by (user_id, object_name);

A5 = foreach A4 generate group.user_id as user_id,
                         group.object_name as object_name,
                         COUNT(A3) as object_count;

B = group A5 by user_id;
describe B;

C = foreach B {
  C1 = order A5 by object_count desc, object_name asc;
  generate flatten(Stitch(Over(C1, 'row_number'), C1));
};
describe C;

D = foreach C generate stitched::user_id as user_id,
                       stitched::object_name as object_name,
                       stitched::object_count as object__count,
                       $0 as row_id,
                       CONCAT(CONCAT(CONCAT(stitched::object_name, ' ('), (chararray)object_count), ')') as object_with_count;
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
--dump E2;

--store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
--  'BatchProcessResult:BP1 BatchProcessResult:timestamp');


store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
  'BatchProcessResult:BP4');





