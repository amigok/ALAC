-- 2021-09-15 [DBI-318]

--

set data_retention_length_in_days = 3;
set day_macro_eval = '{{ ds }}';
set cutoff_date = etl_batch_date_add_day($day_macro_eval,-$data_retention_length_in_days+1);
set cutoff_tag = etl_batch_tag_add_day($day_macro_eval,-$data_retention_length_in_days+1);



delete from bidrequest_ingest_initial                       where $cutoff_date < current_date() and event_timestamp < $cutoff_date;
delete from adserver_ingest_initial                         where $cutoff_date < current_date() and event_timestamp < $cutoff_date;
delete from beacon_ingest_initial                           where $cutoff_date < current_date() and event_timestamp < $cutoff_date and source_bucket is null; -- DBI-1161
delete from etl_str_adserver_production_stage_2_src_type    where to_date($cutoff_tag) < current_date() and etl_batch_tag < $cutoff_tag;

-- 2022-09-26 retention changed on  from 7 days to 3 for below tables
-- delete from RAW_LOG_ENRICHED_BID_RESPONSE_V2                where $cutoff_date < current_date() and bid_request_day < $cutoff_date;
delete from RAW_LOG_ENRICHED_BID_RESPONSE_SEAT              where $cutoff_date < current_date() and event_timestamp < $cutoff_date;


