--register /usr/lib/hbase/lib/*.jar;

%declare stime $time_start

/**/

register /usr/hdp/2.2.0.0-2041/pig/piggybank.jar;
define Stitch org.apache.pig.piggybank.evaluation.Stitch;
define Over org.apache.pig.piggybank.evaluation.Over('int');


A = load '/user/hdfs/ReddoorExportDb002' using PigStorage('\u0001')
as (log_id:long, user_id:chararray, domain_name:chararray,
    url:chararray, ip_address:chararray, dump_time:chararray,
    referer:chararray, session_id:chararray, page_title:chararray,
    sex:chararray, age:chararray, live_city:chararray, ec_id:chararray,
    product_id:chararray, name:chararray, price:chararray,
    ec_catalog_name:chararray, pg_catalog_name:chararray,
    object_name:chararray, brand_name:chararray, web_name:chararray,
    web_type:chararray);

A2 = foreach A generate user_id, name,
                        CONCAT(CONCAT(CONCAT(CONCAT(ec_id, '_'), product_id), '_'), name) as ec_product_name,
                        DaysBetween(CurrentTime(), ToDate(dump_time, 'yyyy-MM-dd HH:mm:ss.SSS')) as date_diff;

A3 = filter A2 by (date_diff >= 0) and (date_diff < $m)
    and (name is not null) and (not name matches 'null');


B = group A3 by (user_id, ec_product_name);

B2 = foreach B generate group.user_id as user_id,
                        group.ec_product_name as ec_product_name,
                        COUNT(A3) as name_count;


C = group B2 by user_id;

C2 = foreach C {
    CC = order B2 by name_count desc, ec_product_name asc;
    generate flatten(Stitch(Over(CC, 'row_number'), CC));
};

C3 = filter C2 by ($0 <= $n);

C4 = foreach C3 generate stitched::user_id as user_id,
                         $0 as row_id,
                         CONCAT(CONCAT(CONCAT(ec_product_name, ' ('), (chararray)name_count), ')') as name_with_count;


D = group C4 by user_id;

D2 = foreach D {
    DD = order C4 by row_id asc;
    generate CONCAT($0, CONCAT('_',(chararray)'$stime')) as key, BagToString(DD.name_with_count, ',') as value;
};


store D2 into 'hbase://mdays' using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'BatchProcessResult:BP3');

