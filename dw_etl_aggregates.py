from datetime import datetime, timedelta
from airflow import DAG
from hooks.centralized_task_failure_alerts import CentralizedTaskFailureAlerts
from hooks.pager_duty_alert_hook import task_failure_alert as pager_duty_task_failure_alert
from hooks.slack_alert_hook import SlackAlertHook
from operators.snowflake_sql_sensor import SnowflakeSqlSensor
from operators.snowflake_sql import SnowflakeSQL
from operators.snowflake_zero_rows_qc import SnowflakeZeroRowsQC

start_date = datetime(2021, 12, 16, 13, 0)
schedule_interval = "0 * * * *"

slack = SlackAlertHook()
centralized_failure_callbacks = CentralizedTaskFailureAlerts([
    {'func':pager_duty_task_failure_alert, 'conditional_task_context': 'send_page'},
    {'func':slack.task_failure}
])

# -- Check for new commits on repository

default_args = {
    "owner": "dw_etl",
    "start_date": start_date,
    "email": ["datateam@sharethrough.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "priority_weight": 25,
    "on_failure_callback": centralized_failure_callbacks.fire_alerts
}

dag = DAG(
    dag_id="dw_etl_aggregates",
    tags=["dw_etl","dwa", "impact_medium"],
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=7,
    user_defined_macros=dict(
        slack_channel='#eng-data-alerts'
    ),
)

conn_id_insert = 'east_snowflake_airflow_wh_s'
conn_id_sensor_qc = 'airflow_sensor_qc_wh'
conn_id_sensor_qc_xl = 'east_snowflake_airflow_wh_xl'

# ==================================================================================
# ------ Referential integrity check

qc_fatal_dsp_dimension = SnowflakeZeroRowsQC(
   dag=dag,
   task_id="qc_fatal_dsp_dimension",
   sql="sql/dw_etl_aggregates/qc_fatal_dsp_dimension.sql",
   snowflake_conn_id=conn_id_sensor_qc,
   depends_on_past=False,
   retries=1,
)


qc_fatal_adomain_mapping = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_fatal_adomain_mapping",
    sql="sql/dw_etl_aggregates/qc_fatal_adomain_mapping.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_fees = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_fees",
    sql="sql/dw_etl_aggregates/qc_warning_fees.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_impression_request_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_impression_request_fact",
    sql="sql/dw_etl_aggregates/qc_warning_impression_request_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_bid_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_bid_fact",
    sql="sql/dw_etl_aggregates/qc_warning_bid_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_bid_error_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_bid_error_fact",
    sql="sql/dw_etl_aggregates/qc_warning_bid_error_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_beacon_metric_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_beacon_metric_fact",
    sql="sql/dw_etl_aggregates/qc_warning_beacon_metric_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_ft_uid_sampled_metrics = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_ft_uid_sampled_metrics",
    sql="sql/dw_etl_aggregates/qc_warning_ft_uid_sampled_metrics.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)


qc_warning_multi_level_impression_request_supply = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_multi_level_impression_request_supply",
    sql="sql/dw_etl_aggregates/qc_warning_multi_level_impression_request_supply.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)


qc_warning_multi_level_bid_demand = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_multi_level_bid_demand",
    sql="sql/dw_etl_aggregates/qc_warning_multi_level_bid_demand.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

"""
qc_warning_beacon_metric_fact_dimension_testing = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_beacon_metric_fact_dimension_testing",
    sql="sql/dw_etl_aggregates/qc_warning_beacon_metric_fact_dimension_testing.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
"""

qc_warning_bid_request_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_bid_request_fact",
    sql="sql/dw_etl_aggregates/qc_warning_bid_request_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc_xl,
    depends_on_past=False,
    retries=1,
)

qc_warning_throttled_bid_request_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_throttled_bid_request_fact",
    sql="sql/dw_etl_aggregates/qc_warning_throttled_bid_request_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_bid_response_seat_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_bid_response_seat_fact",
    sql="sql/dw_etl_aggregates/qc_warning_bid_response_seat_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
""""
qc_warning_all_domains_metric_fact = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_all_domains_metric_fact",
    sql="sql/dw_etl_aggregates/qc_warning_all_domains_metric_fact.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
"""

qc_fatal_country_dimension = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_fatal_country_dimension",
    sql="sql/dw_etl_aggregates/qc_fatal_country_dimension.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_fatal_sfp_etl_v_sfp_placement_domains = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_fatal_sfp_etl_v_sfp_placement_domains",
    sql="sql/dw_etl_aggregates/qc_fatal_sfp_etl_v_sfp_placement_domains.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_warning_etl_audience_segments = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_etl_audience_segments",
    sql="sql/dw_etl_aggregates/qc_warning_etl_audience_segments.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)



# ------


check_raw_log_impression_requests = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_impression_requests",
    sql="sql/dw_etl_aggregates/check_raw_log_impression_requests.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_fraud_detection_lookup_success = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_fraud_detection_lookup_success",
    sql="sql/dw_etl_aggregates/check_raw_log_fraud_detection_lookup_success.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_request_filters = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_request_filters",
    sql="sql/dw_etl_aggregates/check_raw_log_request_filter.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(30+1),
    execution_timeout=timedelta(seconds=800),
    retries=30,
)

check_raw_log_bid_request = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_bid_request",
    sql="sql/dw_etl_aggregates/check_raw_log_bid_request.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_smart_throttling = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_smart_throttling",
    sql="sql/dw_etl_aggregates/check_raw_log_smart_throttling.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(30+1),
    execution_timeout=timedelta(seconds=800),
    retries=30,
)

check_etl_tree_auction_bid_response_joined = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_etl_tree_auction_bid_response_joined",
    sql="sql/dw_etl_aggregates/check_etl_tree_auction_bid_response_joined.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)


check_raw_log_enriched_bid_response_seat_bid = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_enriched_bid_response_seat_bid",
    sql="sql/dw_etl_aggregates/check_raw_log_enriched_bid_response_seat_bid.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)


check_raw_beacon_pivot_arid_duration_threshold = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_pivot_arid_duration_threshold",
    sql="sql/dw_etl_aggregates/check_raw_beacon_pivot_arid_duration_threshold.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_pivot_arid_value = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_pivot_arid_value",
    sql="sql/dw_etl_aggregates/check_raw_beacon_pivot_arid_value.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_arid_user_event_click = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_arid_user_event_click",
    sql="sql/dw_etl_aggregates/check_raw_beacon_arid_user_event_click.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_pivot_arid = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_pivot_arid",
    sql="sql/dw_etl_aggregates/check_raw_beacon_pivot_arid.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_pivot_arid_share = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_pivot_share",
    sql="sql/dw_etl_aggregates/check_raw_beacon_pivot_arid_share.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_metric_financial_total_billable_and_price = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_metric_financial_total_billable_and_price",
    sql="sql/dw_etl_aggregates/check_metric_financial_total_billable_and_price.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_upstream_metric_financial_publisher_revenue = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_upstream_metric_financial_publisher_revenue",
    sql="sql/dw_etl_aggregates/check_upstream_metric_financial_publisher_revenue.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_upstream_metric_financial_fees = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_upstream_metric_financial_fees",
    sql="sql/dw_etl_aggregates/check_upstream_metric_financial_fees.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_pivot_arid_attribute = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_pivot_arid_attribute",
    sql="sql/dw_etl_aggregates/check_raw_beacon_pivot_arid_attribute.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_bid_error = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_bid_error",
    sql="sql/dw_etl_aggregates/check_raw_log_bid_error.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_audience_segments_flattened = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_audience_segments_flattened",
    sql="sql/dw_etl_aggregates/check_raw_log_audience_segments_flattened.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_log_dsp_to_receive_request = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_dsp_to_receive_request",
    sql="sql/dw_etl_aggregates/check_raw_log_dsp_to_receive_request.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

check_raw_beacon_arid_is_experiment = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_beacon_arid_is_experiment",
    sql="sql/dw_etl_aggregates/check_raw_beacon_arid_is_experiment.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=800*(20+1),
    execution_timeout=timedelta(seconds=800),
    retries=20,
)

# ------ added 04112024 for incremental load of deals data

check_raw_log_bid_request_imp = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_bid_request_imp",
    sql="sql/dw_etl_aggregates/check_raw_log_bid_request_imp.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

# ==================================================================================
# INSERTS

# ----- impression request

impression_request_fact = SnowflakeSQL(
    dag=dag,
    task_id="impression_request_fact",
    sql="sql/dw_etl_aggregates/impression_request_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

impression_request_fact.set_upstream(check_raw_log_impression_requests)
impression_request_fact.set_upstream(check_raw_log_fraud_detection_lookup_success)
impression_request_fact.set_upstream(check_raw_log_request_filters)

# ----- bid requests

bid_request_fact = SnowflakeSQL(
    dag=dag,
    task_id="bid_request_fact",
    sql="sql/dw_etl_aggregates/bid_request_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

bid_request_fact.set_upstream(check_raw_log_impression_requests)
bid_request_fact.set_upstream(check_raw_log_bid_request)
bid_request_fact.set_upstream(check_raw_log_dsp_to_receive_request)

# ----- throttled bid requests

throttled_bid_request_fact = SnowflakeSQL(
    dag=dag,
    task_id="throttled_bid_request_fact",
    sql="sql/dw_etl_aggregates/throttled_bid_request_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

throttled_bid_request_fact.set_upstream(check_raw_log_impression_requests)
throttled_bid_request_fact.set_upstream(check_raw_log_smart_throttling)


# ----- seat bid requests

bid_response_seat_fact = SnowflakeSQL(
    dag=dag,
    task_id="bid_response_seat_fact",
    sql="sql/dw_etl_aggregates/bid_response_seat_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

bid_response_seat_fact.set_upstream(check_raw_log_impression_requests)
bid_response_seat_fact.set_upstream(check_raw_log_enriched_bid_response_seat_bid)
bid_response_seat_fact.set_upstream(check_raw_log_bid_request)
bid_response_seat_fact.set_upstream(check_raw_log_dsp_to_receive_request)


# ----- bids

bid_fact = SnowflakeSQL(
    dag=dag,
    task_id="bid_fact",
    sql="sql/dw_etl_aggregates/bid_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

bid_fact.set_upstream(check_raw_log_impression_requests)
bid_fact.set_upstream(check_raw_log_enriched_bid_response_seat_bid)
bid_fact.set_upstream(check_raw_log_bid_request)
bid_fact.set_upstream(check_raw_log_dsp_to_receive_request)


# ----- bid error

bid_error_fact = SnowflakeSQL(
    dag=dag,
    task_id="bid_error_fact",
    sql="sql/dw_etl_aggregates/bid_error_fact.sql",
    snowflake_conn_id=conn_id_insert,
)

bid_error_fact.set_upstream(check_raw_log_enriched_bid_response_seat_bid)
bid_error_fact.set_upstream(check_raw_log_impression_requests)
bid_error_fact.set_upstream(check_raw_log_bid_error)


# ----- beacon metrics

beacon_metric_fact = SnowflakeSQL(
    dag=dag,
    task_id="beacon_metric_fact",
    sql="sql/dw_etl_aggregates/beacon_metric_fact.sql",
    snowflake_conn_id=conn_id_insert,
)



# ----- etl_beacon_adserver_joined

etl_beacon_adserver_joined = SnowflakeSQL(
    dag=dag,
    task_id="etl_beacon_adserver_joined",
    sql="sql/dw_etl_aggregates/etl_beacon_adserver_joined.sql",
    snowflake_conn_id=conn_id_insert,
)


# ----- etl_audience_segments

etl_audience_segments = SnowflakeSQL(
    dag=dag,
    task_id="etl_audience_segments",
    sql="sql/dw_etl_aggregates/etl_audience_segments.sql",
    snowflake_conn_id=conn_id_insert,
)

etl_audience_segments.set_upstream(check_etl_tree_auction_bid_response_joined)
etl_audience_segments.set_upstream(check_raw_log_audience_segments_flattened)


ft_ortb_health_beacon_metrics = SnowflakeSQL(
    dag=dag,
    task_id="ft_ortb_health_beacon_metrics",
    sql="sql/dw_etl_aggregates/ft_ortb_health_beacon_metrics.sql",
    snowflake_conn_id=conn_id_insert,
)

ft_ortb_health_beacon_metrics.set_upstream(check_etl_tree_auction_bid_response_joined)
ft_ortb_health_beacon_metrics.set_upstream(check_raw_log_audience_segments_flattened)

etl_beacon_adserver_joined.set_upstream(etl_audience_segments)
etl_beacon_adserver_joined.set_upstream(check_raw_log_impression_requests)
etl_beacon_adserver_joined.set_upstream(check_raw_log_enriched_bid_response_seat_bid)
etl_beacon_adserver_joined.set_upstream(check_raw_log_bid_request)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_pivot_arid_value)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_arid_user_event_click)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_pivot_arid)
etl_beacon_adserver_joined.set_upstream(check_metric_financial_total_billable_and_price)
etl_beacon_adserver_joined.set_upstream(check_upstream_metric_financial_publisher_revenue)
etl_beacon_adserver_joined.set_upstream(check_upstream_metric_financial_fees)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_pivot_arid_duration_threshold)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_pivot_arid_attribute)
etl_beacon_adserver_joined.set_upstream(check_raw_beacon_pivot_arid_share)
etl_beacon_adserver_joined.set_upstream(check_etl_tree_auction_bid_response_joined)
etl_beacon_adserver_joined.set_upstream(check_raw_log_dsp_to_receive_request)

beacon_metric_fact.set_upstream(etl_beacon_adserver_joined)

beacon_metric_fact_lite = SnowflakeSQL(
    dag=dag,
    task_id="beacon_metric_fact_lite",
    sql="sql/dw_etl_aggregates/beacon_metric_fact_lite.sql",
    snowflake_conn_id=conn_id_insert,
)
beacon_metric_fact >> beacon_metric_fact_lite
"""
beacon_metric_fact_dimension_testing = SnowflakeSQL(
    dag=dag,
    task_id="beacon_metric_fact_dimension_testing",
    sql="sql/dw_etl_aggregates/beacon_metric_fact_dimension_testing.sql",
    snowflake_conn_id=conn_id_insert,
)

beacon_metric_fact_dimension_testing.set_upstream(check_raw_log_impression_requests)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_log_enriched_bid_response_seat_bid)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_log_bid_request)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_pivot_arid_value)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_arid_user_event_click)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_pivot_arid)
beacon_metric_fact_dimension_testing.set_upstream(check_metric_financial_total_billable_and_price)
beacon_metric_fact_dimension_testing.set_upstream(check_upstream_metric_financial_publisher_revenue)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_pivot_arid_duration_threshold)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_pivot_arid_attribute)
beacon_metric_fact_dimension_testing.set_upstream(check_raw_beacon_pivot_arid_share)
beacon_metric_fact_dimension_testing.set_upstream(check_etl_tree_auction_bid_response_joined)
"""
ft_all_domains_metrics = SnowflakeSQL(
    dag=dag,
    task_id="ft_all_domains_metrics",
    sql="sql/dw_etl_aggregates/ft_all_domains_metrics.sql",
    snowflake_conn_id=conn_id_insert,
)

impression_request_fact >> ft_all_domains_metrics
beacon_metric_fact >> ft_all_domains_metrics

# ----- Multi level Impression Request Supply

multi_level_impression_request_supply = SnowflakeSQL(
    dag=dag,
    task_id="multi_level_impression_request_supply",
    sql="sql/dw_etl_aggregates/multi_level_impression_request_supply.sql",
    snowflake_conn_id=conn_id_insert,
)

impression_request_fact >> multi_level_impression_request_supply
bid_request_fact >> multi_level_impression_request_supply
throttled_bid_request_fact >> multi_level_impression_request_supply
bid_response_seat_fact >> multi_level_impression_request_supply
bid_fact >> multi_level_impression_request_supply
beacon_metric_fact >> multi_level_impression_request_supply
ft_all_domains_metrics >> multi_level_impression_request_supply

multi_level_impression_request_supply >> qc_warning_multi_level_impression_request_supply

# ----- Multi level Bid Demand

multi_level_bid_demand = SnowflakeSQL(
    dag=dag,
    task_id="multi_level_bid_demand",
    sql="sql/dw_etl_aggregates/multi_level_bid_demand.sql",
    snowflake_conn_id=conn_id_insert,
)

bid_fact >> multi_level_bid_demand
beacon_metric_fact >> multi_level_bid_demand

multi_level_bid_demand >> qc_warning_multi_level_bid_demand

# ----- ft_beacon_metric_experiment

ft_beacon_metric_experiment = SnowflakeSQL(
    dag=dag,
    task_id="ft_beacon_metric_experiment",
    sql="sql/dw_etl_aggregates/ft_beacon_metric_experiment.sql",
    snowflake_conn_id=conn_id_insert,
)

ft_beacon_metric_experiment.set_upstream(check_raw_beacon_arid_is_experiment)
ft_beacon_metric_experiment.set_upstream(check_raw_beacon_pivot_arid_value)
ft_beacon_metric_experiment.set_upstream(check_raw_beacon_arid_user_event_click)
ft_beacon_metric_experiment.set_upstream(check_raw_beacon_pivot_arid)
ft_beacon_metric_experiment.set_upstream(check_metric_financial_total_billable_and_price)
ft_beacon_metric_experiment.set_upstream(check_raw_beacon_pivot_arid_attribute)
ft_beacon_metric_experiment.set_upstream(check_etl_tree_auction_bid_response_joined)


ft_uid_sampled_metrics = SnowflakeSQL(
    dag=dag,
    task_id="ft_uid_sampled_metrics",
    sql="sql/dw_etl_aggregates/ft_uid_sampled_metrics.sql",
    snowflake_conn_id=conn_id_insert,
)

ft_uid_sampled_metrics.set_upstream(etl_beacon_adserver_joined)


agg_dsp_targeting_performance = SnowflakeSQL(
    dag=dag,
    task_id="agg_dsp_targeting_performance",
    sql="sql/dw_etl_aggregates/agg_dsp_targeting_performance.sql",
    snowflake_conn_id=conn_id_insert,
)

agg_dsp_targeting_performance.set_upstream(ft_uid_sampled_metrics)

agg_dsp_deal_bid_performance_hourly = SnowflakeSQL(
    dag=dag,
    task_id="agg_dsp_deal_bid_performance_hourly",
    sql="sql/dw_etl_aggregates/agg_dsp_deal_bid_performance_hourly.sql",
    snowflake_conn_id=conn_id_insert,
)

agg_dsp_deal_bid_performance_hourly.set_upstream(check_raw_log_bid_request_imp)

beacon_metric_fact >> agg_dsp_deal_bid_performance_hourly
bid_request_fact >> agg_dsp_deal_bid_performance_hourly
bid_fact >> agg_dsp_deal_bid_performance_hourly

# ==================================================================================

# -- ----- Fatal QC checks ----
#  Here, we wish to stop the entire DAG is one of these QC tests fail

qc_fatal_dsp_dimension.set_downstream(check_raw_log_impression_requests)
qc_fatal_dsp_dimension.set_downstream(check_raw_log_fraud_detection_lookup_success)
qc_fatal_dsp_dimension.set_downstream(check_raw_log_request_filters)
qc_fatal_dsp_dimension.set_downstream(check_etl_tree_auction_bid_response_joined)
qc_fatal_dsp_dimension.set_downstream(check_raw_log_enriched_bid_response_seat_bid)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_pivot_arid_value)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_arid_user_event_click)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_pivot_arid)
qc_fatal_dsp_dimension.set_downstream(check_metric_financial_total_billable_and_price)
qc_fatal_dsp_dimension.set_downstream(check_upstream_metric_financial_publisher_revenue)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_pivot_arid_duration_threshold)
qc_fatal_dsp_dimension.set_downstream(check_raw_log_bid_request)
qc_fatal_dsp_dimension.set_downstream(check_raw_log_smart_throttling)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_pivot_arid_attribute)
qc_fatal_dsp_dimension.set_downstream(check_raw_beacon_pivot_arid_share)

qc_fatal_adomain_mapping.set_downstream(check_raw_log_impression_requests)
qc_fatal_adomain_mapping.set_downstream(check_raw_log_fraud_detection_lookup_success)
qc_fatal_adomain_mapping.set_downstream(check_raw_log_request_filters)
qc_fatal_adomain_mapping.set_downstream(check_etl_tree_auction_bid_response_joined)
qc_fatal_adomain_mapping.set_downstream(check_raw_log_enriched_bid_response_seat_bid)
qc_fatal_adomain_mapping.set_downstream(check_raw_beacon_pivot_arid_value)
qc_fatal_adomain_mapping.set_downstream(check_raw_beacon_arid_user_event_click)
qc_fatal_adomain_mapping.set_downstream(check_raw_beacon_pivot_arid)
qc_fatal_adomain_mapping.set_downstream(check_metric_financial_total_billable_and_price)
qc_fatal_adomain_mapping.set_downstream(check_upstream_metric_financial_publisher_revenue)
qc_fatal_adomain_mapping.set_downstream(check_raw_beacon_pivot_arid_duration_threshold)
qc_fatal_adomain_mapping.set_downstream(check_raw_log_bid_request)
qc_fatal_adomain_mapping.set_downstream(check_raw_log_smart_throttling)
qc_fatal_adomain_mapping.set_downstream(check_raw_beacon_pivot_arid_attribute)

qc_fatal_country_dimension.set_downstream(check_raw_log_impression_requests)
qc_fatal_country_dimension.set_downstream(check_raw_log_fraud_detection_lookup_success)
qc_fatal_country_dimension.set_downstream(check_raw_log_request_filters)
qc_fatal_country_dimension.set_downstream(check_etl_tree_auction_bid_response_joined)
qc_fatal_country_dimension.set_downstream(check_raw_log_enriched_bid_response_seat_bid)
qc_fatal_country_dimension.set_downstream(check_raw_beacon_pivot_arid_value)
qc_fatal_country_dimension.set_downstream(check_raw_beacon_arid_user_event_click)
qc_fatal_country_dimension.set_downstream(check_raw_beacon_pivot_arid)
qc_fatal_country_dimension.set_downstream(check_metric_financial_total_billable_and_price)
qc_fatal_country_dimension.set_downstream(check_upstream_metric_financial_publisher_revenue)
qc_fatal_country_dimension.set_downstream(check_raw_beacon_pivot_arid_duration_threshold)
qc_fatal_country_dimension.set_downstream(check_raw_log_bid_request)
qc_fatal_country_dimension.set_downstream(check_raw_log_smart_throttling)
qc_fatal_country_dimension.set_downstream(check_raw_beacon_pivot_arid_attribute)



qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_impression_requests)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_fraud_detection_lookup_success)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_request_filters)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_etl_tree_auction_bid_response_joined)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_enriched_bid_response_seat_bid)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_beacon_pivot_arid_value)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_beacon_arid_user_event_click)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_beacon_pivot_arid)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_metric_financial_total_billable_and_price)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_upstream_metric_financial_publisher_revenue)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_beacon_pivot_arid_duration_threshold)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_bid_request)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_log_smart_throttling)
qc_fatal_sfp_etl_v_sfp_placement_domains.set_downstream(check_raw_beacon_pivot_arid_attribute)


# -- ------ QC check warnings ----
# Here, we set QC checks that are not critical enough to stop the entire dag, but for which we
# still need to get notified and act

qc_warning_impression_request_fact.set_upstream(impression_request_fact)
qc_warning_bid_fact.set_upstream(bid_fact)
qc_warning_bid_request_fact.set_upstream(bid_request_fact)
qc_warning_throttled_bid_request_fact.set_upstream(throttled_bid_request_fact)
qc_warning_beacon_metric_fact.set_upstream(beacon_metric_fact)
#qc_warning_beacon_metric_fact_dimension_testing.set_upstream(beacon_metric_fact_dimension_testing)
qc_warning_bid_response_seat_fact.set_upstream(bid_response_seat_fact)
qc_warning_bid_error_fact.set_upstream(bid_error_fact)
qc_warning_fees.set_upstream(check_upstream_metric_financial_fees)
#qc_warning_all_domains_metric_fact.set_upstream(all_domains_metric_fact)
qc_warning_ft_uid_sampled_metrics.set_upstream(ft_uid_sampled_metrics)
qc_warning_etl_audience_segments.set_upstream(etl_audience_segments)
