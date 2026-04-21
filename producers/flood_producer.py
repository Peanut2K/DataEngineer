"""
producers/flood_producer.py
────────────────────────────
Produces daily flood risk events to the Kafka topic `flood_raw`.

Data source priority:
  1. CSV file at data/flood_risk.csv  (supply your own or generate via --generate)
  2. Mock data                        (auto-generated and saved to data/ if no CSV)

Key features:
  - Daily incremental load : skips processing if today has already been produced
                             (state stored in logs/flood_state.json)
  - Idempotency            : deterministic message_id (MD5 of date + province +
                             risk level) used as the Kafka record key
"""

import csv
import hashlib
import json
import os
import random
from datetime import date, datetime, timezone
from typing import List, Optional

from kafka import KafkaProducer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    PROVINCES,
)
from utils.logger import get_logger

logger = get_logger("flood_producer")

_STATE_FILE     = os.path.join("logs", "flood_state.json")
_CSV_FILE       = os.path.join("data", "flood_risk.csv")
_RISK_LEVELS    = ["Low", "Medium", "High", "Very High"]
# Probability weights for each risk level (realistic skew toward lower risk)
_RISK_WEIGHTS   = [0.45, 0.30, 0.18, 0.07]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _message_id(date_str: str, province: str, flood_risk: str) -> str:
    raw = f"{date_str}|{province}|{flood_risk}"
    return hashlib.md5(raw.encode()).hexdigest()


def _load_last_date() -> Optional[str]:
    try:
        with open(_STATE_FILE, "r") as fh:
            return json.load(fh).get("last_date")
    except FileNotFoundError:
        return None


def _save_last_date(date_str: str) -> None:
    os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
    with open(_STATE_FILE, "w") as fh:
        json.dump({"last_date": date_str}, fh)


# ─────────────────────────────────────────────────────────────────────────────
# Mock / CSV data
# ─────────────────────────────────────────────────────────────────────────────

def generate_mock_flood_data(target_date: str) -> List[dict]:
    """Generate one flood-risk record per province for the given date."""
    records = []
    for province in PROVINCES:
        risk = random.choices(_RISK_LEVELS, weights=_RISK_WEIGHTS)[0]
        records.append({
            "date":              target_date,
            "province":          province["name"],
            "flood_risk":        risk,
            "affected_area_km2": round(random.uniform(0, 500), 2),
            "water_level_m":     round(random.uniform(0.1, 5.0), 2),
            "source":            "mock",
        })
    return records


def _save_to_csv(records: List[dict]) -> None:
    """Persist generated mock records to CSV for auditability."""
    os.makedirs(os.path.dirname(_CSV_FILE), exist_ok=True)
    file_exists = os.path.exists(_CSV_FILE)

    with open(_CSV_FILE, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=records[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    logger.info(f"Flood mock data appended to {_CSV_FILE}")


def _read_csv(target_date: str) -> List[dict]:
    """Read today's records from an existing CSV."""
    records: List[dict] = []
    with open(_CSV_FILE, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("date") == target_date:
                records.append(dict(row))
    return records


def _collect_records(target_date: str) -> List[dict]:
    """Return flood records for target_date from CSV or generate mock data."""
    records: List[dict] = []

    if os.path.exists(_CSV_FILE):
        records = _read_csv(target_date)
        if records:
            logger.info(f"Loaded {len(records)} flood records from CSV for {target_date}")
            return records

    # No CSV / no data for today → generate mock
    logger.info(f"Generating mock flood data for {target_date}")
    records = generate_mock_flood_data(target_date)
    _save_to_csv(records)
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer
# ─────────────────────────────────────────────────────────────────────────────

def _create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_block_ms=60_000,
        api_version=(3, 9, 0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_producer() -> None:
    """
    Produce daily flood risk data to Kafka.
    Implements daily incremental load + idempotency.
    """
    logger.info("Flood Risk Producer — starting")
    topic      = KAFKA_TOPICS["flood"]
    today      = date.today().isoformat()
    last_date  = _load_last_date()

    # ── Incremental guard — skip if today was already produced ────────────────
    if last_date == today:
        logger.info(f"Flood data already produced for {today} — skipping")
        return

    records  = _collect_records(today)
    producer = _create_producer()
    sent     = 0

    try:
        ingested_at = datetime.now(timezone.utc).isoformat()
        for record in records:
            record["ingested_at"] = ingested_at
            msg_id = _message_id(record["date"], record["province"], record["flood_risk"])
            record["message_id"] = msg_id

            producer.send(topic, key=msg_id, value=record)
            sent += 1

        producer.flush()
        logger.info(f"Flood Producer — sent {sent} messages to '{topic}'")
        _save_last_date(today)

    except Exception as exc:
        logger.error(f"Flood Producer — error: {exc}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
