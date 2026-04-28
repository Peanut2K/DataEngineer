"""
producers/weather_producer.py
─────────────────────────────
Produces temperature / weather events to the Kafka topic `weather_raw`.

Data source priority:
  1. OpenWeatherMap API  (requires OPENWEATHER_API_KEY in .env)
  2. Mock data           (used automatically when no API key is configured)

Key features:
  - Incremental load  : state tracked in logs/weather_state.json
  - Idempotency       : deterministic message_id (MD5 of timestamp + province +
                        temperature) used as the Kafka record key
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
    OPENWEATHER_API_KEY,
    OPENWEATHER_BASE_URL,
    PROVINCES,
)
from utils.logger import get_logger
from utils.retry import with_retry

logger = get_logger("weather_producer")

_STATE_FILE = os.path.join("logs", "weather_state.json")

def _message_id(timestamp: str, province: str, temperature: float) -> str:
    raw = f"{timestamp}|{province}|{temperature}"
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
# Mock data
# ─────────────────────────────────────────────────────────────────────────────

# Realistic base temperatures per province (Thailand context)
_BASE_TEMPS = {
    "Bangkok":   30,
    "ChiangMai": 26,
    "Phuket":    29,
    "KhonKaen":  28,
    "Chon Buri": 29,
}


def _mock_weather() -> List[dict]:
    """Generate mock weather data for all provinces."""
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for province in PROVINCES:
        base   = _BASE_TEMPS.get(province["name"], 28)
        temp   = round(base + random.uniform(-3, 5), 1)
        humid  = round(random.uniform(55, 95), 1)
        cond   = random.choice(["Clear", "Clouds", "Rain", "Thunderstorm", "Drizzle"])
        _MOCK_DESC = {
            "Clear":       "clear sky",
            "Clouds":      "scattered clouds",
            "Rain":        "moderate rain",
            "Thunderstorm": "thunderstorm with rain",
            "Drizzle":     "light intensity drizzle",
        }

        records.append({
            "timestamp":           now,
            "province":            province["name"],
            "temperature":         temp,
            "humidity":            humid,
            "weather_condition":   cond,
            "weather_description": _MOCK_DESC.get(cond, cond.lower()),
            "source":              "mock",
        })
    return records


# ─────────────────────────────────────────────────────────────────────────────
# OpenWeatherMap API fetch
# ─────────────────────────────────────────────────────────────────────────────

@with_retry(max_retries=3, delay=5, exceptions=(requests.RequestException,))
def _fetch_openweather(province: dict) -> Optional[dict]:
    """Fetch current weather for one province from OpenWeatherMap (city-name query)."""
    params = {
        "q":     province["weather_query"],
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",   # Celsius
    }
    resp = requests.get(
        f"{OPENWEATHER_BASE_URL}/weather",
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    return {
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "province":          province["name"],
        "temperature":       data["main"]["temp"],
        "humidity":          data["main"]["humidity"],
        "weather_condition":   data["weather"][0]["main"]        if data.get("weather") else None,
        "weather_description": data["weather"][0]["description"] if data.get("weather") else None,
        "source":              "openweather",
    }


def _collect_records() -> List[dict]:
    """Return records from API or fall back to mock data."""
    if not OPENWEATHER_API_KEY:
        logger.warning("OPENWEATHER_API_KEY not set — using mock weather data")
        return _mock_weather()

    records: List[dict] = []
    for province in PROVINCES:
        try:
            record = _fetch_openweather(province)
            if record:
                records.append(record)
        except Exception as exc:
            logger.error(f"Failed to fetch weather for {province['name']}: {exc}")

    # Fall back to mock if API returned nothing
    if not records:
        logger.warning("No API data received — falling back to mock weather data")
        return _mock_weather()

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
    """Fetch weather data and produce messages to Kafka."""
    logger.info("Weather Producer — starting")
    topic          = KAFKA_TOPICS["weather"]
    last_timestamp = _load_last_timestamp()
    latest_seen: Optional[str] = None

    records  = _collect_records()
    producer = _create_producer()
    sent     = 0

    try:
        for record in records:
            # Skip records older than or equal to the last run (incremental)
            if last_timestamp and record["timestamp"] <= last_timestamp:
                continue

            msg_id = _message_id(record["timestamp"], record["province"], record["temperature"])
            record["message_id"] = msg_id

            producer.send(topic, key=msg_id, value=record)
            sent += 1

            if latest_seen is None or record["timestamp"] > latest_seen:
                latest_seen = record["timestamp"]

        producer.flush()
        logger.info(f"Weather Producer — sent {sent} messages to '{topic}'")

        if latest_seen:
            _save_last_timestamp(latest_seen)

    except Exception as exc:
        logger.error(f"Weather Producer — error: {exc}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
