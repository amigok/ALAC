-- DBI-2135
-- dw_etl_aggregates_daily/dsp_deals_bid_performance.sql

set batch_tag  = '{{ ds }}';   -- string to use for batching
set start_hour = dateadd('hour', -1, $batch_tag); -- specifically for impression requests to pad with additional hour
set end_hour = dateadd('hour', -1, dateadd('day', 1, $batch_tag)); -- specifically for impression requests to pad with additional hour


--- analytics.dw.agg_dsp_deal_bid_performance ---

begin;

delete from analytics.dw.agg_dsp_deal_bid_performance where etl_batch_tag = $batch_tag;

insert into analytics.dw.agg_dsp_deal_bid_performance (
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
        -- ----------------------------------
        placement_domain_key,
        country_code,
        user_agent_device_category,
        user_agent_browser
    from analytics.dw_access.vw_dimensional_impression_request_to_auction
    where impression_request_hour between $start_hour and $end_hour
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
    where rlbri.bid_request_day = $batch_tag
    group by 1,2,3,4,5,6,7,8,9
),
bid_request_daily_dsp_deal as (
    select
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
    group by 1,2,3,4,5,6,7
),
bid_response_metrics_daily_dsp_deal as (
    select
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
    where event_day = $batch_tag
    group by 1,2,3,4,5,6,7
),
beacon_metric_daily_dsp_deal as (
    select
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
    where event_day = $batch_tag
    group by 1,2,3,4,5,6,7
)
select
    $batch_tag as etl_batch_tag,
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
    breq.event_day = bresp.event_day and
    breq.source_id = bresp.source_id and
    breq.deal_id = bresp.deal_id and
    breq.placement_domain_key = bresp.placement_domain_key and
    breq.country_code = bresp.country_code and
    breq.user_agent_device_category = bresp.user_agent_device_category and
    breq.user_agent_browser = bresp.user_agent_browser
full join beacon_metric_daily_dsp_deal bmf
on
    breq.event_day = bmf.event_day and
    breq.source_id = bmf.source_id and
    breq.deal_id = bmf.deal_id and
    breq.placement_domain_key = bmf.placement_domain_key and
    breq.country_code = bmf.country_code and
    breq.user_agent_device_category = bmf.user_agent_device_category and
    breq.user_agent_browser = bmf.user_agent_browser
group by 1,2,3,4,5,6,7,8
;

commit;