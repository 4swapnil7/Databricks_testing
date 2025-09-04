-- refresh > keeps ddl, updates data, keeps checkpoint
-- replace > if ddl to be updated of source, clears old checkpoint, reprocess data from start

--volume is used as a var defined in config of pipeline settings
--when defining materialised view, don't use from stream even if it's a streaming table

create or replace streaming table workspace.default.stream_bronze1
select * 
from stream read_files(
  --'/Volumes/workspace/default/course2',
  "${volume}",
  format => 'parquet');

  create or replace streaming table workspace.default.stream_silver_expectations1
  (
    constraint valid_name expect (emp_name in ('atul','roshan','rohit')),
    constraint valid_age expect (age > 23) on violation drop row
    --constraint valid_order_id expect (order_id is not null) on violation fail update
  )
  as
  select *,
  date_format(
         from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'),
         'yyyy-MM-dd HH:mm:ss'
       ) AS processing_time
  --,_metadata.file_name as source_file
  from stream stream_bronze1;

  create or refresh materialized view mv_gold_agg
  select emp_name, count(course_id) as total_courses_completed
  from stream_silver_expectations1
  where course_completed = 'Y'
  group by emp_name;
