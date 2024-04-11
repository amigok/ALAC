-- check_upstream_raw_log_impression_requests.sql

with this_one_day as (
  select
    impression_request_hour,
    count(1) as rows_for_hour
  from DW_ACCESS.vw_raw_log_impression_requests_to_auction
  where impression_request_day = '{{ ds }}'
  group by 1
) -- ----------------------------------
, count_the_hours as (
    select count(1) as count_of_hours_with_enough_data
    from this_one_day
    where rows_for_hour >= 2000000
) -- ----------------------------------
select to_char(count_of_hours_with_enough_data) as count_of_hours_with_enough_data
from count_the_hours
where count_of_hours_with_enough_data = 24
;
