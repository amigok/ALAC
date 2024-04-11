from datetime import datetime, timedelta
from airflow import DAG
from hooks.centralized_task_failure_alerts import CentralizedTaskFailureAlerts
from hooks.pager_duty_alert_hook import task_failure_alert as pager_duty_task_failure_alert
from hooks.slack_alert_hook import SlackAlertHook
from operators.snowflake_sql_sensor import SnowflakeSqlSensor
from operators.snowflake_sql import SnowflakeSQL
from operators.snowflake_zero_rows_qc import SnowflakeZeroRowsQC

start_date = datetime(2022, 5, 1, 0, 0)
schedule_interval = "5 2 * * *"

slack = SlackAlertHook()
centralized_failure_callbacks = CentralizedTaskFailureAlerts([
    {'func':pager_duty_task_failure_alert, 'conditional_task_context': 'send_page'},
    {'func':slack.task_failure}
])

default_args = {
    "owner": "dw_etl",
    "start_date": start_date,
    "email": ["datateam@sharethrough.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "priority_weight": 25,
    "on_failure_callback": centralized_failure_callbacks.fire_alerts,
    #"on_retry_callback": slack.task_retry
}

dag = DAG(
    dag_id="dw_etl_aggregates_daily",
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

# ==================================================================================

check_sharethrough_exchanges_pro_forma_supply_fact = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_sharethrough_exchanges_pro_forma_supply_fact",
    sql="sql/dw_etl_aggregates_daily/check_sharethrough_exchanges_pro_forma_supply_fact.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_beacon_metric_fact_lite = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_beacon_metric_fact_lite",
    sql="sql/dw_etl_aggregates_daily/check_beacon_metric_fact_lite.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_sae_etl_v_qrcodescan_fact = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_sae_etl_v_qrcodescan_fact",
    sql="sql/dw_etl_aggregates_daily/check_sae_etl_v_qrcodescan_fact.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_beacon_metric_fact = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_beacon_metric_fact",
    sql="sql/dw_etl_aggregates_daily/check_beacon_metric_fact.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_bid_fact = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_bid_fact",
    sql="sql/dw_etl_aggregates_daily/check_bid_fact.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_bid_request_fact = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_bid_request_fact",
    sql="sql/dw_etl_aggregates_daily/check_bid_request_fact.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_raw_log_bid_request_imp = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_bid_request_imp",
    sql="sql/dw_etl_aggregates_daily/check_raw_log_bid_request_imp.sql",
    conn_id="airflow_sensor_qc_wh",
    depends_on_past=False,
    mode='reschedule',
    poke_interval=300,
    timeout=600 * (60 + 1),
    execution_timeout=timedelta(seconds=600),
    retries=60,
)

check_raw_log_impression_requests = SnowflakeSqlSensor(
    dag=dag,
    task_id="check_raw_log_impression_requests",
    sql="sql/dw_etl_aggregates_daily/check_raw_log_impression_requests.sql",
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

primary_publisher_retention = SnowflakeSQL(
    dag=dag,
    task_id="primary_publisher_retention",
    sql="sql/dw_etl_aggregates_daily/primary_publisher_retention.sql",
    snowflake_conn_id=conn_id_insert,
)

primary_publisher_retention.set_upstream(check_sharethrough_exchanges_pro_forma_supply_fact)

qc_fatal_primary_publisher_retention = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_fatal_primary_publisher_retention",
    sql="sql/dw_etl_aggregates_daily/qc_fatal_primary_publisher_retention.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)

qc_fatal_primary_publisher_retention.set_upstream(primary_publisher_retention)


etl_dsp_seat_daily_publisher_earnings_gross_revenue = SnowflakeSQL(
    dag=dag,
    task_id="etl_dsp_seat_daily_publisher_earnings_gross_revenue",
    sql="sql/dw_etl_aggregates_daily/etl_dsp_seat_daily_publisher_earnings_gross_revenue.sql",
    snowflake_conn_id=conn_id_insert,
)
etl_dsp_seat_daily_publisher_earnings_gross_revenue.set_upstream(check_beacon_metric_fact_lite)


qc_etl_dsp_seat_daily_publisher_earnings_gross_revenue = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_etl_dsp_seat_daily_publisher_earnings_gross_revenue",
    sql="sql/dw_etl_aggregates_daily/qc_etl_dsp_seat_daily_publisher_earnings_gross_revenue.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
qc_etl_dsp_seat_daily_publisher_earnings_gross_revenue.set_upstream(etl_dsp_seat_daily_publisher_earnings_gross_revenue)


trader_seat_retention = SnowflakeSQL(
    dag=dag,
    task_id="trader_seat_retention",
    sql="sql/dw_etl_aggregates_daily/trader_seat_retention.sql",
    snowflake_conn_id=conn_id_insert,
)
trader_seat_retention.set_upstream(qc_etl_dsp_seat_daily_publisher_earnings_gross_revenue)



qc_fatal_trader_seat_retention = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_fatal_trader_seat_retention",
    sql="sql/dw_etl_aggregates_daily/qc_fatal_trader_seat_retention.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
qc_fatal_trader_seat_retention.set_upstream(trader_seat_retention)

trader_seat_retention_status = SnowflakeSQL(
    dag=dag,
    task_id="trader_seat_retention_status",
    sql="sql/dw_etl_aggregates_daily/trader_seat_retention_status.sql",
    snowflake_conn_id=conn_id_insert,
)
trader_seat_retention_status.set_upstream(qc_fatal_trader_seat_retention)

agg_daily_qrcode_metrics = SnowflakeSQL(
    dag=dag,
    task_id="agg_daily_qrcode_metrics",
    sql="sql/dw_etl_aggregates_daily/agg_daily_qrcode_metrics.sql",
    snowflake_conn_id=conn_id_insert,
)
agg_daily_qrcode_metrics.set_upstream(check_beacon_metric_fact_lite)
agg_daily_qrcode_metrics.set_upstream(check_sae_etl_v_qrcodescan_fact)

qc_warning_agg_daily_qrcode_metrics = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_agg_daily_qrcode_metrics",
    sql="sql/dw_etl_aggregates_daily/qc_warning_agg_daily_qrcode_metrics.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
qc_warning_agg_daily_qrcode_metrics.set_upstream(agg_daily_qrcode_metrics)


dim_daily_dsp_discrepancy_adjustment = SnowflakeSQL(
    dag=dag,
    task_id="dim_daily_dsp_discrepancy_adjustment",
    sql="sql/dw_etl_aggregates_daily/dim_daily_dsp_discrepancy_adjustment.sql",
    snowflake_conn_id=conn_id_insert,
)

qc_warning_dim_daily_dsp_discrepancy_adjustment = SnowflakeZeroRowsQC(
    dag=dag,
    task_id="qc_warning_dim_daily_dsp_discrepancy_adjustment",
    sql="sql/dw_etl_aggregates_daily/qc_warning_dim_daily_dsp_discrepancy_adjustment.sql",
    snowflake_conn_id=conn_id_sensor_qc,
    depends_on_past=False,
    retries=1,
)
qc_warning_dim_daily_dsp_discrepancy_adjustment.set_upstream(dim_daily_dsp_discrepancy_adjustment)


agg_dsp_deal_bid_performance = SnowflakeSQL(
    dag=dag,
    task_id="agg_dsp_deal_bid_performance",
    sql="sql/dw_etl_aggregates_daily/agg_dsp_deal_bid_performance.sql",
    snowflake_conn_id=conn_id_insert,
)
agg_dsp_deal_bid_performance.set_upstream(check_beacon_metric_fact)
agg_dsp_deal_bid_performance.set_upstream(check_bid_fact)
agg_dsp_deal_bid_performance.set_upstream(check_bid_request_fact)
agg_dsp_deal_bid_performance.set_upstream(check_raw_log_bid_request_imp)


agg_audience_deal_bid_performance = SnowflakeSQL(
    dag=dag,
    task_id="agg_audience_deal_bid_performance",
    sql="sql/dw_etl_aggregates_daily/agg_audience_deal_bid_performance.sql",
    snowflake_conn_id=conn_id_insert,
)
agg_audience_deal_bid_performance.set_upstream(agg_dsp_deal_bid_performance)


ft_ortb_health_metrics = SnowflakeSQL(
    dag=dag,
    task_id="ft_ortb_health_metrics",
    sql="sql/dw_etl_aggregates_daily/ft_ortb_health_metrics.sql",
    snowflake_conn_id=conn_id_insert,
)

ft_ortb_health_metrics.set_upstream(check_raw_log_impression_requests)
