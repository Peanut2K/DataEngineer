"""
seed_from_silver.py
────────────────────
Restore BOTH Silver and Gold layers from a silver_environmental_joined JSON export.

Writes to:
  - silver_environmental_joined  (upsert by province + date)
  - fact_environmental           (upsert by province + date, with full risk scoring)
  - dim_location, dim_time

Usage:
    python seed_from_silver.py path/to/silver_environmental_joined.json

The JSON file is the raw mongoexport format: one document per line (NDJSON).
Each line may contain the MongoDB _id field which will be stripped.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.operations import UpdateOne

from config.settings import GOLD_COLLECTIONS, MONGODB_DATABASE, MONGODB_URI, PROVINCES
from transformations.silver_to_gold import (
    calculate_risk,
    _build_dim_location,
    _build_dim_time,
    _setup_gold_indexes,
)
from utils.logger import get_logger

logger = get_logger("seed_from_silver")

# ── Province → coordinates lookup ─────────────────────────────────────────────
_PROVINCE_COORDS = {p["name"]: {"lat": p["lat"], "lon": p["lon"]} for p in PROVINCES}

# ── Same helpers as silver_to_gold.py ─────────────────────────────────────────

def _heat_index(temp, humidity):
    if temp is None or humidity is None:
        return None
    if temp < 27 or humidity < 40:
        return round(temp, 1)
    hi = (
        -8.78469475556
        + 1.61139411 * temp
        + 2.33854883889 * humidity
        - 0.14611605 * temp * humidity
        - 0.012308094 * temp ** 2
        - 0.0164248277778 * humidity ** 2
        + 0.002211732 * temp ** 2 * humidity
        + 0.00072546 * temp * humidity ** 2
        - 0.000003582 * temp ** 2 * humidity ** 2
    )
    return round(hi, 1)


def _pm25_category(pm25):
    if pm25 is None:
        return "Unknown"
    if pm25 <= 25:
        return "Good"
    if pm25 <= 37:
        return "Moderate"
    if pm25 <= 50:
        return "Unhealthy for Sensitive Groups"
    if pm25 <= 90:
        return "Unhealthy"
    return "Very Unhealthy"


def _humidity_level(humidity):
    if humidity is None:
        return "Unknown"
    if humidity < 40:
        return "Low"
    if humidity <= 70:
        return "Normal"
    return "High"


def _weather_pm25_impact(weather_condition):
    if not weather_condition:
        return "Unknown"
    cond = weather_condition.lower()
    if any(w in cond for w in ["rain", "drizzle", "thunderstorm", "shower"]):
        return "Lower"
    if any(w in cond for w in ["clear", "sunny", "hot"]):
        return "Higher"
    if any(w in cond for w in ["cloud", "overcast", "mist", "fog", "haze"]):
        return "Moderate"
    return "Neutral"


def _build_fact_record(silver: dict) -> dict:
    """Convert one silver_environmental_joined record → Gold fact record."""
    pm25              = silver.get("pm25")
    temperature       = silver.get("temperature")
    humidity          = silver.get("humidity")
    flood_risk        = silver.get("flood_risk")
    weather_condition = silver.get("weather_condition")
    province          = silver.get("province")

    scores = calculate_risk(pm25, temperature, flood_risk)
    coords = _PROVINCE_COORDS.get(province, {})

    return {
        "province":    province,
        "date":        silver.get("date"),
        "lat":         coords.get("lat"),
        "lon":         coords.get("lon"),

        # Measures
        "pm25":              pm25,
        "temperature":       temperature,
        "humidity":          humidity,
        "weather_condition": weather_condition,
        "weather_description": silver.get("weather_description"),
        "flood_risk":        flood_risk,
        "affected_area_km2": silver.get("affected_area_km2"),
        "water_level_m":     silver.get("water_level_m"),

        # Risk scoring
        "pm25_score":  scores["pm25_score"],
        "temp_score":  scores["temp_score"],
        "flood_score": scores["flood_score"],
        "risk_score":  scores["risk_score"],
        "risk_level":  scores["risk_level"],

        # Multi-dimensional analysis
        "pm25_category":       _pm25_category(pm25),
        "heat_index":          _heat_index(temperature, humidity),
        "humidity_level":      _humidity_level(humidity),
        "weather_pm25_impact": _weather_pm25_impact(weather_condition),
        "is_rainy": weather_condition is not None and any(
            w in (weather_condition or "").lower()
            for w in ["rain", "drizzle", "thunderstorm", "shower"]
        ),

        # Metadata
        "hour_bucket":   silver.get("hour_bucket"),
        "_processed_at": datetime.now(timezone.utc).isoformat(),
        "_layer":        "gold",
        "_seeded_from":  "silver_environmental_joined",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_JSON = Path(__file__).parent / "data" / "seed" / "silver_environmental_joined.json"


def main():
    if len(sys.argv) >= 2:
        json_path = Path(sys.argv[1])
    else:
        json_path = _DEFAULT_JSON

    if not json_path.exists():
        print(f"File not found: {json_path}")
        sys.exit(1)

    # Read NDJSON (one MongoDB document per line)
    silver_records = []
    with open(json_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            doc.pop("_id", None)   # remove MongoDB _id — MongoDB will assign new one
            silver_records.append(doc)

    logger.info(f"Loaded {len(silver_records)} records from {json_path.name}")

    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]

    # 1. Setup indexes
    _setup_gold_indexes(db)

    # 2. Build dim_location
    _build_dim_location(db)

    # 3. Build dim_time for every unique date in the data
    dates = {r["date"] for r in silver_records if r.get("date")}
    for date_str in sorted(dates):
        _build_dim_time(db, date_str)
    logger.info(f"dim_time: upserted {len(dates)} dates: {sorted(dates)}")

    # 4. Upsert into silver_environmental_joined (restore Silver layer)
    silver_coll = db["silver_environmental_joined"]
    silver_ops = []
    for rec in silver_records:
        if not rec.get("province") or not rec.get("date"):
            continue
        # Mark as seeded so we can distinguish from live pipeline records
        rec_with_meta = {**rec, "_seeded_from": "json_export", "_layer": "silver"}
        silver_ops.append(
            UpdateOne(
                {"province": rec["province"], "date": rec["date"]},
                {"$set": rec_with_meta},
                upsert=True,
            )
        )
    if silver_ops:
        result = silver_coll.bulk_write(silver_ops, ordered=False)
        logger.info(
            f"silver_environmental_joined — inserted {result.upserted_count} new, "
            f"updated {result.modified_count} existing"
        )

    # 5. Build and upsert fact records (Gold layer)
    fact_coll = db[GOLD_COLLECTIONS["fact"]]
    ops = []
    for silver in silver_records:
        if not silver.get("province") or not silver.get("date"):
            logger.warning(f"Skipping record missing province/date: {silver}")
            continue
        record = _build_fact_record(silver)
        ops.append(
            UpdateOne(
                {"province": record["province"], "date": record["date"]},
                {"$set": record},
                upsert=True,
            )
        )

    if ops:
        result = fact_coll.bulk_write(ops, ordered=False)
        logger.info(
            f"fact_environmental — inserted {result.upserted_count} new, "
            f"updated {result.modified_count} existing"
        )
    else:
        logger.warning("No valid records to insert")

    client.close()
    print(f"\n✅ Seeded {len(ops)} records")
    print(f"   silver_environmental_joined: {len(silver_ops)} records")
    print(f"   fact_environmental:          {len(ops)} records")
    print(f"   Dates: {sorted(dates)}")


if __name__ == "__main__":
    main()
