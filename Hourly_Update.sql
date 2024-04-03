-- this script updated 04022024 by Amit.G

set batch_tag  = '{{ ts }}'; 
set max_batch_tag = (select nvl(max(etl_batch_tag), '{{ ts }}') from analytics.sandbox.agg_dsp_deal_bid_performance);
set start_hour = (select iff($batch_tag > $max_batch_tag, $batch_tag,concat(date_trunc('day', $batch_tag::date),'T00:00:00')::timestamp_ntz));
set end_hour = (select iff($batch_tag > $max_batch_tag, $batch_tag, iff (
                       date_trunc('day', $max_batch_tag::date) = date_trunc('day', $batch_tag::date),
                        $max_batch_tag, concat(date_trunc('day', $batch_tag::date),'T23:00:00')::timestamp_ntz)                     
                   ));

--set batch_tag  = ts_macro_to_etl_batch_tag('2024-04-01T02:00:00+00:00'); 
--set max_batch_tag = (select nvl(max(etl_batch_tag), ts_macro_to_etl_batch_tag('2024-03-30T23:00:00+00:00')) from analytics.sandbox.agg_dsp_deal_bid_performance);
--set start_hour = (select iff($batch_tag > $max_batch_tag, $batch_tag,(select min(event_hour) as event_hour from analytics.dw.beacon_metric_fact where event_day = date_trunc('day', $batch_tag::date))));

BEGIN;

/* 
STEP 1 
Create temporary table
*/

create or replace temporary table analytics.sandbox.WRK_AGG_DSP_DEAL_BID_PERFORMANCE (
	ETL_BATCH_TAG VARCHAR(16777216) NOT NULL,
	EVENT_DAY DATE NOT NULL,
	SOURCE_ID VARCHAR(16777216) NOT NULL,
	DEAL_ID VARCHAR(16777216) NOT NULL,
	PLACEMENT_DOMAIN_KEY VARCHAR(16777216) NOT NULL,
    COUNTRY_CODE VARCHAR(16777216),
    USER_AGENT_DEVICE_CATEGORY VARCHAR(16777216),
    user_agent_browser VARCHAR(16777216),
	BID_REQUEST_COUNT NUMBER(38,0) NOT NULL,
	NATIVE_BID_REQUEST_COUNT NUMBER(38,0),
	VIDEO_BID_REQUEST_COUNT NUMBER(38,0),
	BANNER_BID_REQUEST_COUNT NUMBER(38,0),
    BID_COUNT NUMBER(38,0) NOT NULL,
	BID_AMOUNT_USD NUMBER(20,10),
    IMPRESSION_RENDERED_COUNT NUMBER(38,0) NOT NULL,
	SERVER_IMPRESSION_COUNT NUMBER(38,0) NOT NULL,
	EXCHANGE_GROSS_REVENUE_USD NUMBER(16,5)
--	constraint PK_WRK_AGG_DSP_DEAL_BID_PERFORMANCE primary key (EVENT_DAY, SOURCE_ID, DEAL_ID, PLACEMENT_DOMAIN_KEY, COUNTRY_CODE,USER_AGENT_DEVICE_CATEGORY, user_agent_browser)
);

/*
 STEP 2 
Create daily data for defined hour
*/
insert into analytics.sandbox.WRK_AGG_DSP_DEAL_BID_PERFORMANCE (
    etl_batch_tag,
    event_day,
    source_id,
    deal_id,
    placement_domain_key,
    country_code,
    user_agent_device_category,
    user_agent_browser,
    -- ----------------------------------
    bid_request_count,
    native_bid_request_count,
    video_bid_request_count,
    banner_bid_request_count,
    bid_count,
    bid_amount_usd,
    impression_rendered_count,
    server_impression_count,
    exchange_gross_revenue_usd
)
with
ir as (
    select
        adserver_request_id,
        placement_domain_key,
        country_code,
        user_agent_device_category,
        user_agent_browser
    from analytics.dw_access.vw_dimensional_impression_request_to_auction
    where impression_request_hour between $start_hour and $end_hour
--    where impression_request_hour = $batch_tag
),
bid_request_processing as (
-- flatten the imp object to derive the deal_id
-- deal_id can appear multiple times in an ARID, we need to use this cte to
-- dedup before deriving metrics.
    select
        rlbri.bid_request_hour as event_hour,
        rlbri.bid_request_day as event_day,
        rlbri.adserver_request_id,
        rlbri.endpoint_source_id as source_id,
        nvl(f.value:id::String, '[no_deal_id]') as deal_id,
        nvl(ir.placement_domain_key, '[unknown_placement_domain]') as placement_domain_key,
        nvl(ir.country_code, 'unknown_country') as country_code,
        nvl(ir.user_agent_device_category, '[unknown_user_agent_device_category]') as user_agent_device_category,
        nvl(ir.user_agent_browser, '[unknown_user_agent_browser]') as user_agent_browser,
        max(to_number(rlbri.has_native_bid_request)) as has_native_bid_request,
        max(to_number(rlbri.has_video_bid_request)) as has_video_bid_request,
        max(to_number(rlbri.has_banner_bid_request)) as has_banner_bid_request
    from analytics.dw.raw_log_bid_request_imp rlbri
    left join ir
    on rlbri.adserver_request_id = ir.adserver_request_id,
    lateral flatten( input => rlbri.imp_pmp_json, path => 'deals', outer => true) f
--    where rlbri.bid_request_day = $batch_tag
      where rlbri.bid_request_hour between $start_hour and $end_hour
    group by 1,2,3,4,5,6,7,8,9
),
bid_request_daily_dsp_deal as (
    select
        event_hour,
        event_day,
        source_id,
        deal_id,
        placement_domain_key,
        country_code,
        user_agent_device_category,
        user_agent_browser,
        -- ----------------------------------
        count(*) * inverse_sample_rate_bid_request($batch_tag) as bid_request_count,
        sum(has_native_bid_request) * inverse_sample_rate_bid_request($batch_tag) as native_bid_request_count,
        sum(has_video_bid_request) * inverse_sample_rate_bid_request($batch_tag) as video_bid_request_count,
        sum(has_banner_bid_request) * inverse_sample_rate_bid_request($batch_tag) as banner_bid_request_count
    from bid_request_processing
    group by 1,2,3,4,5,6,7,8
),
bid_response_metrics_daily_dsp_deal as (
    select
        event_hour,
        event_day,
        source_id,
        deal_id,
        placement_domain_key,
        country_code,
        user_agent_device_category,
        user_agent_browser,
        -- ----------------------------------
        sum(bid_count) as bid_count,
        sum(bid_amount_usd) as bid_amount_usd
    from analytics.dw.bid_fact
--    where event_day = $batch_tag
    where event_hour between $start_hour and $end_hour
    group by 1,2,3,4,5,6,7,8
),
beacon_metric_daily_dsp_deal as (
    select
        event_hour,
        event_day,
        source_id,
        deal_id,
        placement_domain_key,
        country_code,
        user_agent_device_category,
        user_agent_browser,
        -- ----------------------------------
        sum(impression_rendered_count) as impression_rendered_count,
        sum(server_impression_count) as server_impression_count,
        sum(exchange_gross_revenue_usd) as exchange_gross_revenue_usd
    from analytics.dw.beacon_metric_fact
--    where event_day = $batch_tag
    where event_hour between $start_hour and $end_hour
    group by 1,2,3,4,5,6,7,8
)
select
--    $batch_tag as etl_batch_tag,
    case
             when $batch_tag > $max_batch_tag
             then $batch_tag
             else 
                iff (
                       date_trunc('day', $max_batch_tag::date) = date_trunc('day', $batch_tag::date), $max_batch_tag, $end_hour)                     
         end as etl_batch_tag,
--    coalesce(breq.event_hour, bmf.event_hour, bresp.event_hour) as event_hour,
    coalesce(breq.event_day, bmf.event_day, bresp.event_day) as event_day,
    coalesce(breq.source_id, bmf.source_id, bresp.source_id) as source_id,
    coalesce(breq.deal_id, bmf.deal_id, bresp.deal_id) as deal_id,
    coalesce(breq.placement_domain_key, bmf.placement_domain_key, bresp.placement_domain_key) as placement_domain_key,
    coalesce(breq.country_code, bmf.country_code, bresp.country_code) as country_code,
    coalesce(breq.user_agent_device_category, bmf.user_agent_device_category, bresp.user_agent_device_category) as user_agent_device_category,
    coalesce(breq.user_agent_browser, bmf.user_agent_browser, bresp.user_agent_browser) as user_agent_browser,
    -- ----------------------------------
    nvl(sum(breq.bid_request_count),0) as bid_request_count,
    nvl(sum(breq.native_bid_request_count),0) as native_bid_request_count,
    nvl(sum(breq.video_bid_request_count),0) as video_bid_request_count,
    nvl(sum(breq.banner_bid_request_count),0) as banner_bid_request_count,
    nvl(sum(bresp.bid_count),0) as bid_count,
    nvl(sum(bresp.bid_amount_usd),0) as bid_amount_usd,
    nvl(sum(bmf.impression_rendered_count),0) as impression_rendered_count,
    nvl(sum(bmf.server_impression_count),0) as server_impression_count,
    nvl(sum(bmf.exchange_gross_revenue_usd),0) as exchange_gross_revenue_usd
from bid_request_daily_dsp_deal breq
full join bid_response_metrics_daily_dsp_deal bresp
on
--    breq.event_day = bresp.event_day and
    breq.event_hour = bresp.event_hour and
    breq.source_id = bresp.source_id and
    breq.deal_id = bresp.deal_id and
    breq.placement_domain_key = bresp.placement_domain_key and
    breq.country_code = bresp.country_code and
    breq.user_agent_device_category = bresp.user_agent_device_category and
    breq.user_agent_browser = bresp.user_agent_browser
full join beacon_metric_daily_dsp_deal bmf
on
--    breq.event_day = bmf.event_day and
    breq.event_hour = bmf.event_hour and
    breq.source_id = bmf.source_id and
    breq.deal_id = bmf.deal_id and
    breq.placement_domain_key = bmf.placement_domain_key and
    breq.country_code = bmf.country_code and
    breq.user_agent_device_category = bmf.user_agent_device_category and 
    breq.user_agent_browser = bmf.user_agent_browser
/*
where -- if $batch_tag > $max_batch_tag load a single day
              (
                  $batch_tag > $max_batch_tag and
                  bmf.event_hour = $batch_tag
              ) 
              or
              -- else load from beginning of the day in $batch_tag to either $max_batch_tag, if $max_batch_tag in
              -- same day as $batch_tag, else $batch_tag
                (
                   $batch_tag <= $max_batch_tag and
                   bmf.event_hour between
                   -- from start of day
                   (select min(event_hour) as event_hour from analytics.dw.beacon_metric_fact where event_day = date_trunc('day', $batch_tag::date)) and
                   -- either $max_batch_tag if $max_batch_tag in same month as $batch_tag, or end of month in $batch_tag
                   iff(
                       date_trunc('day', $max_batch_tag::date) = date_trunc('day', $batch_tag::date),
                       $max_batch_tag, (select max(event_hour) as event_hour from analytics.dw.beacon_metric_fact where event_day = date_trunc('day', $batch_tag::date))
                   )
                 ) 
*/
group by 1,2,3,4,5,6,7,8
;

/*
select event_day, sum(EXCHANGE_GROSS_REVENUE_USD) sand_EXCHANGE_GROSS_REVENUE_USD from analytics.sandbox.WRK_AGG_DSP_DEAL_BID_PERFORMANCE 
--where event_day in ('2024-03-31','2024-04-01')
group by 1
--select distinct event_day from analytics.sandbox.WRK_AGG_DSP_DEAL_BID_PERFORMANCE limit 10
*/


/* 
STEP 3
Perform UPSERT (Update else Insert) operation using hourly data as source and aggregate table as target
*/

MERGE INTO analytics.sandbox.agg_dsp_deal_bid_performance a 
USING (select * from analytics.sandbox.WRK_AGG_DSP_DEAL_BID_PERFORMANCE where $batch_tag > $max_batch_tag) b 
      ON  a.event_day = b.event_day and
          a.source_id = b.source_id and
          a.deal_id = b.deal_id and
          a.placement_domain_key = b.placement_domain_key and
          a.country_code = b.country_code and
          a.user_agent_device_category = b.user_agent_device_category and
          a.user_agent_browser = b.user_agent_browser
WHEN MATCHED THEN UPDATE
      SET 
          a.etl_batch_tag = b.etl_batch_tag
          a.bid_request_count = a.bid_request_count+b.bid_request_count,
          a.native_bid_request_count = a.native_bid_request_count + b.native_bid_request_count,
          a.video_bid_request_count = a.video_bid_request_count + b.video_bid_request_count,
          a.banner_bid_request_count = a.banner_bid_request_count + b.banner_bid_request_count,
          a.bid_count = a.bid_count + b.bid_count,
          a.bid_amount_usd = a.bid_amount_usd + b.bid_amount_usd,
          a.impression_rendered_count = a.impression_rendered_count + b.impression_rendered_count,
          a.server_impression_count = a.server_impression_count + b.server_impression_count,
          a.exchange_gross_revenue_usd = a.exchange_gross_revenue_usd + b.exchange_gross_revenue_usd
WHEN NOT MATCHED 
        THEN INSERT 
          (etl_batch_tag,
           event_day,
           source_id,
           deal_id,
           placement_domain_key,
           country_code,
           user_agent_device_category,
           user_agent_browser,
           bid_request_count,
           native_bid_request_count,
           video_bid_request_count,
           banner_bid_request_count,
           bid_count,
           bid_amount_usd,
           impression_rendered_count,
           server_impression_count,
           exchange_gross_revenue_usd) 
        Values
           (b.etl_batch_tag,
            b.event_day,
            b.source_id,
            b.deal_id,
            b.placement_domain_key,
            b.country_code,
            b.user_agent_device_category,
            b.user_agent_browser,
            b.bid_request_count,
            b.native_bid_request_count,
            b.video_bid_request_count,
            b.banner_bid_request_count,
            b.bid_count,
            b.bid_amount_usd,
            b.impression_rendered_count,
            b.server_impression_count,
            b.exchange_gross_revenue_usd)
;

commit;

--begin

/* Step 4
Delete dates being rebuilt for exception cases (reruns, restarts)
*/
 
delete
    from analytics.sandbox.agg_dsp_deal_bid_performance
 where $batch_tag <= $max_batch_tag and
--       date_trunc('day', etl_batch_tag::date) = date_trunc('day', $batch_tag::date)
       event_day = (select distinct event_day from analytics.sandbox.WRK_agg_dsp_deal_bid_performance)
;

/* Step 5
Insert dates for exception cases (reruns, restarts)
*/

insert into analytics.sandbox.agg_dsp_deal_bid_performance (
           etl_batch_tag,
           event_day,
           source_id,
           deal_id,
           placement_domain_key,
           country_code,
           user_agent_device_category,
           user_agent_browser,
           bid_request_count,
           native_bid_request_count,
           video_bid_request_count,
           banner_bid_request_count,
           bid_count,
           bid_amount_usd,
           impression_rendered_count,
           server_impression_count,
           exchange_gross_revenue_usd
)
    select etl_batch_tag,
           event_day,
           source_id,
           deal_id,
           placement_domain_key,
           country_code,
           user_agent_device_category,
           user_agent_browser,
           bid_request_count,
           native_bid_request_count,
           video_bid_request_count,
           banner_bid_request_count,
           bid_count,
           bid_amount_usd,
           impression_rendered_count,
           server_impression_count,
           exchange_gross_revenue_usd
    from analytics.sandbox.WRK_agg_dsp_deal_bid_performance
    where $batch_tag <= $max_batch_tag
;
commit;
