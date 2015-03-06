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
                        DaysBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as date,
                        MilliSecondsBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as ms;

A3 = filter A2 by (date >= 0) and (date < $m) and (object_name is not null)
     and (not object_name matches 'null');

--A32 = filter A3 by user_id == '0007EAB6-0EAF-4FE9-96AE-145DAC62C8CE';
--A32 = filter A3 by user_id == '00044A91-6C92-4145-A8AF-D53D4CA494A2';
--A32 = filter A3 by (user_id == '0007EAB6-0EAF-4FE9-96AE-145DAC62C8CE') or (user_id == '00044A91-6C92-4145-A8AF-D53D4CA494A2');

A33 = foreach A3 generate user_id, dump_time, date, ms, flatten(STRSPLIT(object_name, '\\|')); -- {i}
A331 = foreach A33 generate (chararray)$0, (chararray)$1, (long)$2, (long)$3, (bag{tuple()})TOBAG($4 ..) as bbb:bag{ttt:tuple()};

A34 = foreach A331 generate $0 .. $3, flatten($4);

A341 = foreach A34 generate (chararray)$0 as user_id, (chararray)$1, (long)$2, (long)$3 as ms, (chararray)$4 as object_name;

B = group A341 by (user_id, object_name);

C = foreach B {
  CC = order A341 by ms asc;
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
  generate CONCAT($0, CONCAT('_',(chararray)'$stime')) as key, BagToString(EE.object_name, ',') as value;
}

store E2 into 'hbase://mdays_test' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
  'BatchProcessResult:BP5');





