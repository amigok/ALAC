-- dags/sql/east_snowflake_ad_server_logs/step_10_append_raw_log_experiment_assignment.sql
-- Tue Aug  8 20:37:35 UTC 2017
-- etl_batch_tag is ts (hourly)
-- https://www.pivotaltracker.com/story/show/149814449 added raw_log_experiment_assignment
-- 2021-07-27 [DBI-250] adding query tagging
-- 2023-02-21 [DBI-1636] modified to source directly from adsever_ingest_initial [DBI-1695] release to production

/* raw_log_experiment_assignment */


CALL analytics.dw.add_airflow_query_tag_key('event_type','experimental-group'); -- query tagging

CALL analytics.dw.add_airflow_query_tag_key('table_name','raw_log_experiment_assignment'); -- query tagging
begin;


SET batch_tag = analytics.dw.ts_macro_to_etl_batch_tag('{{ ts }}');
SET batch_hour = analytics.dw.ts_macro_to_hour_timestamp('{{ ts }}');

delete from analytics.dw.raw_log_experiment_assignment where etl_batch_tag = $batch_tag;


INSERT INTO analytics.dw.raw_log_experiment_assignment (
    experiment_assignment_timestamp,
    experiment_assignment_hour,
    experiment_assignment_day,
    etl_batch_tag,
    --
    adserver_request_id,
    experiment_label,
    experiment_group
    --
) SELECT DISTINCT
      try_to_timestamp_ntz(SRC:timestamp::String)                 as experiment_assignment_timestamp,
      trunc(try_to_timestamp_ntz(SRC:timestamp::String), 'HOUR')  as experiment_assignment_hour,
      trunc(try_to_timestamp_ntz(SRC:timestamp::String), 'DAY')   as experiment_assignment_day,
      $batch_tag                                                  as etl_batch_tag,
      --
      SRC:data:adserverRequestId::String    as adserver_request_id,
      experiment.value:label::String        as experiment_label,
      experiment.value:group::String        as experiment_group
 FROM analytics.dw.adserver_ingest_initial
, LATERAL FLATTEN (SRC:data:experiments) experiment
WHERE event_type = 'experimental-group'
  AND event_hour = $batch_hour
;

-- delete rows that duplicate rows already procesed in the previous hour
-- doiing this here at this step lets us avoid doing it also for child tables
delete from analytics.dw.raw_log_experiment_assignment this_hour
 using (select adserver_request_id,
               experiment_label
          from analytics.dw.raw_log_experiment_assignment 
         where etl_batch_tag = etl_batch_tag_add_hour($batch_tag, -1)
       ) previous_hour
 where this_hour.adserver_request_id = previous_hour.adserver_request_id
  and this_hour.experiment_label = previous_hour.experiment_label
  and this_hour.etl_batch_tag = $batch_tag
;

commit;


CALL add_airflow_query_tag_key('table_name','raw_log_experiment_group_labels'); -- query tagging

begin;

delete from analytics.dw.raw_log_experiment_group_labels where EXPERIMENT_ASSIGNMENT_HOUR = $batch_hour;

insert into analytics.dw.raw_log_experiment_group_labels 
with experiment_assignment_concat as (
    select ADSERVER_REQUEST_ID,
           EXPERIMENT_GROUP || ':' || EXPERIMENT_LABEL as EXPERIMENT_GROUP_LABELS
      from analytics.dw.RAW_LOG_EXPERIMENT_ASSIGNMENT
     where EXPERIMENT_ASSIGNMENT_HOUR = $batch_hour

)
    select $batch_hour as EXPERIMENT_ASSIGNMENT_HOUR,
           ADSERVER_REQUEST_ID,
           array_agg(distinct EXPERIMENT_GROUP_LABELS) within group (order by EXPERIMENT_GROUP_LABELS) as EXPERIMENT_GROUP_LABELS
      from experiment_assignment_concat
  group by 1,2
  ;

commit;