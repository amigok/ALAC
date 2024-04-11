-- DBI-2135 - 2023-11-02
-- dw_etl_aggregates_daily/check_bid_request_fact.sql

with this_one_day as (
  select
    event_hour,
    count(1) as rows_per_hour
  from analytics.dw.bid_request_fact
  where event_day = '{{ ds }}' and
        datediff(hour,to_timestamp_ntz('{{ ts }}'::Date),convert_timezone('UTC',current_timestamp)::timestamp_ntz) >= 24
  group by 1
) -- ----------------------------------
, count_the_hours as (
  select count(1) as count_of_hours_with_enough_data
    from this_one_day
  where rows_per_hour >= 150000
) -- ----------------------------------
  select to_char(count_of_hours_with_enough_data) as count_of_hours_with_enough_data
    from count_the_hours
  where count_of_hours_with_enough_data = 24
;