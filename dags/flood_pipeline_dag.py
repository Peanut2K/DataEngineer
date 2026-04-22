"""
dags/flood_pipeline_dag.py
────────────────────────────
Daily Pipeline DAG (fully independent):
  1. produce_flood
  2. wait_for_consumer (20s)
  3. bronze_to_silver  (flood source only)
  4. silver_to_gold    (refreshes gold with latest flood data)

Schedule: every day at midnight (@daily)
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def produce_flood():
    """Push flood risk data for all 5 provinces into Kafka."""
    from producers.flood_producer import run_producer
    run_producer()


def wait_for_consumer():
    """Wait 20s for bronze-consumer to flush Kafka messages into MongoDB."""
    time.sleep(20)


def run_bronze_to_silver():
    """Process flood Bronze → Silver (independent of air/weather pipeline)."""
    from transformations.bronze_to_silver import run_bronze_to_silver
    run_bronze_to_silver(sources=["flood"])


def run_silver_to_gold():
    """Refresh Gold fact table with latest flood data."""
    from transformations.silver_to_gold import run_silver_to_gold
    run_silver_to_gold()


def check_risk_alert():
    """Send flood alert if any province has High or Very High flood risk."""
    from alerts.risk_alert import check_flood_alert
    check_flood_alert()


with DAG(
    dag_id="flood_pipeline_daily",
    description="Flood risk data pipeline — runs once per day",
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule="@daily",
    catchup=False,
    tags=["environmental", "pipeline", "daily"],
) as dag:

    t_flood = PythonOperator(
        task_id="produce_flood",
        python_callable=produce_flood,
    )

    t_wait = PythonOperator(
        task_id="wait_for_consumer",
        python_callable=wait_for_consumer,
    )

    t_b2s = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    t_s2g = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
    )

    t_alert = PythonOperator(
        task_id="check_risk_alert",
        python_callable=check_risk_alert,
    )

    t_flood >> t_wait >> t_b2s >> t_s2g >> t_alert
