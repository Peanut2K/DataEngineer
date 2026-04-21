"""
dags/environmental_pipeline_dag.py
────────────────────────────────────
Hourly Pipeline DAG:
  1. produce_air_quality  ──┐
  2. produce_weather      ──┴──→  3. wait_for_consumer  →  4. bronze_to_silver  →  5. silver_to_gold

Schedule: every hour (@hourly)

Notes:
- KAFKA_BOOTSTRAP_SERVERS must be kafka:29092 (Docker-internal listener)
- MONGODB_URI must be mongodb://admin:password@mongodb:27017 (Docker-internal)
- Both are set as environment variables in docker-compose.yml
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────────────────────────────────────
# Default arguments applied to every task
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ─────────────────────────────────────────────────────────────────────────────
# Task callables
# ─────────────────────────────────────────────────────────────────────────────

def produce_air_quality():
    """Push air quality data for all 5 provinces into Kafka."""
    from producers.air_quality_producer import run_producer
    run_producer()


def produce_weather():
    """Push weather data for all 5 provinces into Kafka."""
    from producers.weather_producer import run_producer
    run_producer()


def wait_for_consumer():
    """
    Wait 20 seconds to allow the bronze-consumer service
    to flush Kafka messages into MongoDB Bronze collections.
    """
    time.sleep(20)


def run_bronze_to_silver():
    """Clean and join Bronze collections (air quality + weather only) → Silver."""
    from transformations.bronze_to_silver import run_bronze_to_silver
    run_bronze_to_silver(sources=["air_quality", "weather"])


def run_silver_to_gold():
    """Score and transform Silver joined → Gold fact table (star schema)."""
    from transformations.silver_to_gold import run_silver_to_gold
    run_silver_to_gold()


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="environmental_pipeline_hourly",
    description="Air quality + weather data pipeline — runs every hour",
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule="@hourly",
    catchup=False,
    tags=["environmental", "pipeline", "hourly"],
) as dag:

    t_air = PythonOperator(
        task_id="produce_air_quality",
        python_callable=produce_air_quality,
    )

    t_weather = PythonOperator(
        task_id="produce_weather",
        python_callable=produce_weather,
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

    # ── Dependency chain ─────────────────────────────────────────────────────
    # air_quality ──┐
    #               ├──→ wait_for_consumer → bronze_to_silver → silver_to_gold
    # weather     ──┘
    [t_air, t_weather] >> t_wait >> t_b2s >> t_s2g
