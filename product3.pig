register /usr/lib/hbase/lib/*.jar;

/**/

register /usr/lib/pig/piggybank.jar;
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
                        DaysBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as date,
                        MilliSecondsBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as ms;

A3 = filter A2 by (date >= 0) and (date < $m) and (object_name is not null)
     and (not object_name matches 'null');

B = group A3 by (user_id, object_name);

C = foreach B {
  CC = order A3 by ms asc;
  generate flatten(Stitch(Over(CC, 'row_number'), CC));
}

C1 = foreach C generate stitched::user_id as user_id,
                        stitched::object_name as object_name,
                        stitched::ms as ms,
                        $0 as row_id;

C2 = filter C1 by (row_id == 1);

D = group C2 by user_id;

D2 = foreach D {
  DD = order C2 by ms asc, object_name asc;
  generate flatten(Stitch(Over(DD, 'row_number'), DD));
}

D3 = foreach D2 generate stitched::user_id as user_id,
                         stitched::object_name as object_name,
                         stitched::ms as ms,
                         $0 as row_id;

D4 = filter D3 by (row_id <= $n);

E = group D4 by user_id;
E2 = foreach E {
  EE = order D4 by row_id asc;
  generate group as key, BagToString(EE.object_name, ',') as value;
}

store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
  'BatchProcessResult:BP5');




