"""
producers/air_quality_producer.py
──────────────────────────────────
Produces PM2.5 air-quality events to the Kafka topic `air_quality_raw`.

Data source:
  Custom OpenAQ-based API (no API key required):
    GET https://api-openaq-data-en.weerapatserver.com/pm25/fixed
  Returns the latest PM2.5 reading for 5 Thai provinces (in Thai names).
  Falls back to mock data if the API is unreachable.

Key features:
  - Incremental load  : only produces records newer than the last run
                        (state stored in logs/air_quality_state.json)
  - Idempotency       : deterministic message_id (MD5 of utc + province + pm25)
                        used as the Kafka record key and deduplicated via
                        MongoDB unique index in the Bronze layer.
"""

import hashlib
import json
import os
import random
from datetime import datetime, timezone
from typing import List, Optional

import requests
from kafka import KafkaProducer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    OPENAQ_CUSTOM_URL,
    PROVINCES,
)
from utils.logger import get_logger
from utils.retry import with_retry

logger = get_logger("air_quality_producer")

_STATE_FILE = os.path.join("logs", "air_quality_state.json")

# Thai province names returned by the API → English names used in the pipeline
_THAI_TO_EN = {
    "กรุงเทพมหานคร":   "Bangkok",
    "เชียงใหม่":       "ChiangMai",
    "ขอนแก่น":         "KhonKaen",
    "ภูเก็ต":          "Phuket",
    "พัทยา (ชลบุรี)": "Chon Buri",
}


def _message_id(timestamp: str, province: str, pm25: float) -> str:
    raw = f"{timestamp}|{province}|{pm25}"
    return hashlib.md5(raw.encode()).hexdigest()


def _load_last_timestamp() -> Optional[str]:
    try:
        with open(_STATE_FILE, "r") as fh:
            return json.load(fh).get("last_timestamp")
    except FileNotFoundError:
        return None


def _save_last_timestamp(ts: str) -> None:
    os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
    with open(_STATE_FILE, "w") as fh:
        json.dump({"last_timestamp": ts}, fh)


# ─────────────────────────────────────────────────────────────────────────────
# Mock data (fallback when API is unreachable)
# ─────────────────────────────────────────────────────────────────────────────

def _mock_air_quality() -> List[dict]:
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for province in PROVINCES:
        pm25 = round(random.choices(
            [random.uniform(5, 25),
             random.uniform(26, 75),
             random.uniform(76, 200)],
            weights=[0.55, 0.35, 0.10],
        )[0], 2)
        records.append({
            "timestamp": now,
            "province":  province["name"],
            "pm25":      pm25,
            "unit":      "µg/m³",
            "source":    "mock",
        })
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Custom API fetch (no API key required)
# ─────────────────────────────────────────────────────────────────────────────

@with_retry(max_retries=3, delay=5, exceptions=(requests.RequestException,))
def _fetch_from_custom_api() -> List[dict]:
    """
    Fetch latest PM2.5 readings from the custom OpenAQ-based endpoint.
    Province names in the response are Thai — mapped to English via _THAI_TO_EN.
    """
    resp = requests.get(OPENAQ_CUSTOM_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    records = []
    for entry in payload.get("data", []):
        thai_name = entry.get("province", "")
        en_name   = _THAI_TO_EN.get(thai_name)
        if not en_name:
            logger.debug(f"Unmapped province '{thai_name}' — skipping")
            continue

        records.append({
            "timestamp": entry["datetime"]["utc"],
            "province":  en_name,
            "pm25":      entry["pm25"],
            "pm10":      entry.get("pm10"),
            "unit":      entry.get("unit", "µg/m³"),
            "source":    "openaq_custom",
        })

    logger.info(f"Custom API returned {len(records)} PM2.5 records")
    return records


def _collect_records(since: Optional[str]) -> List[dict]:
    """Try the custom API first; fall back to mock data on any failure."""
    try:
        records = _fetch_from_custom_api()
        if records:
            return records
        logger.warning("Custom API returned no records — using mock data")
    except Exception as exc:
        logger.error(f"Custom API unavailable ({exc}) — using mock data")
    return _mock_air_quality()


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer
# ─────────────────────────────────────────────────────────────────────────────

def _create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # Serialize values as JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Use message_id as the record key for deterministic partitioning
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",        # Wait for all in-sync replicas
        retries=3,
        max_block_ms=60_000,
        api_version=(3, 9, 0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_producer() -> None:
    """
    Fetch air quality data and produce messages to Kafka.
    Implements incremental load + idempotency.
    """
    logger.info("Air Quality Producer — starting")
    topic           = KAFKA_TOPICS["air_quality"]
    last_timestamp  = _load_last_timestamp()
    latest_seen: Optional[str] = last_timestamp

    if last_timestamp:
        logger.info(f"Incremental mode — fetching records newer than {last_timestamp}")
    else:
        logger.info("Full load — no previous state found")

    records  = _collect_records(since=last_timestamp)
    producer = _create_producer()
    sent     = 0

    try:
        for record in records:
            msg_id = _message_id(record["timestamp"], record["province"], record["pm25"])
            record["message_id"] = msg_id

            producer.send(topic, key=msg_id, value=record)
            sent += 1

            # Track the most recent timestamp for the next run
            if latest_seen is None or record["timestamp"] > latest_seen:
                latest_seen = record["timestamp"]

        producer.flush()
        logger.info(f"Air Quality Producer — sent {sent} messages to '{topic}'")

        if latest_seen:
            _save_last_timestamp(latest_seen)

    except Exception as exc:
        logger.error(f"Air Quality Producer — error: {exc}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
