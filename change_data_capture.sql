create or replace streaming table stream_silver_cdc
comment 'scd type 2 historical data';

apply changes into stream_silver_cdc
 from stream stream_bronze1
 inserts
 keys(course_id)
 apply as delete when course_completed = 'Record_deleted'
 sequence by order_ts
 columns * except (_rescued_data)
 stored as scd type 2;

-- _END_AT being not null means record is inactive and deleted
create or refresh materialized view mv_latest_records
as 
select 
emp_name,
course_id,
order_id,
__START_AT,
__END_AT,
row_number() over (partition by course_id order by __START_AT desc) as latest_record
from stream_silver_cdc
qualify latest_record = 1;
