"""
transformations/bronze_to_silver.py
─────────────────────────────────────
Bronze → Silver transformation layer.

Responsibilities:
  1. Read new records from the three Bronze collections (incremental)
  2. Apply data quality validation (drop invalid records with logging)
  3. Standardise schema for each source
  4. Upsert cleaned records into individual Silver collections
  5. Join all three sources by province + date into silver_environmental_joined

Silver layer principle:
  - Data is cleaned, typed, and validated
  - Duplicates are removed via MongoDB unique indexes
  - Schema is consistent across runs
"""

import json
import os
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.operations import UpdateOne

from config.settings import (
    BRONZE_COLLECTIONS,
    MONGODB_DATABASE,
    MONGODB_URI,
    SILVER_COLLECTIONS,
)
from utils.data_quality import validate_air_quality, validate_weather, validate_flood
from utils.logger import get_logger

logger = get_logger("bronze_to_silver")

_STATE_FILE = os.path.join("logs", "silver_state.json")


# ─────────────────────────────────────────────────────────────────────────────
# State management (incremental processing)
# ─────────────────────────────────────────────────────────────────────────────

def _load_state() -> Optional[str]:
    """Return the ISO timestamp of the last Silver run, or None."""
    try:
        with open(_STATE_FILE, "r") as fh:
            return json.load(fh).get("last_processed")
    except FileNotFoundError:
        return None


def _save_state() -> None:
    """Persist the current UTC timestamp for the next incremental run."""
    os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
    with open(_STATE_FILE, "w") as fh:
        json.dump({"last_processed": datetime.now(timezone.utc).isoformat()}, fh)


# ─────────────────────────────────────────────────────────────────────────────
# Schema helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_hour_bucket(timestamp_str: str) -> str:
    """
    Truncate an ISO timestamp to its hour boundary.
    Example: '2026-04-21T14:37:00+00:00' → '2026-04-21T14:00:00+00:00'
    Used as the join key for hourly data.
    """
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:00:00+00:00")
    except Exception:
        # Fallback: zero out the minutes/seconds characters
        return timestamp_str[:13] + ":00:00+00:00"


# ─────────────────────────────────────────────────────────────────────────────
# Province name normalisation (aliases → canonical GeoJSON names)
# ─────────────────────────────────────────────────────────────────────────────

_PROVINCE_ALIASES: Dict[str, str] = {
    "Pattaya": "Chon Buri",
}


def _normalise_province(name: str) -> str:
    """Return the canonical province name, resolving known aliases."""
    return _PROVINCE_ALIASES.get(name, name)


# ─────────────────────────────────────────────────────────────────────────────
# Record cleaners
# ─────────────────────────────────────────────────────────────────────────────

def _clean_air_quality(raw: dict) -> Optional[dict]:
    """Standardise an air quality Bronze record. Returns None if invalid."""
    cleaned = {
        "province":    _normalise_province((raw.get("province") or "").strip()),
        "timestamp":   raw.get("timestamp"),
        "hour_bucket": _to_hour_bucket(raw.get("timestamp", "")),
        "pm25":        float(raw["pm25"]) if raw.get("pm25") is not None else None,
        "unit":        raw.get("unit", "µg/m³"),
        "message_id":  raw.get("message_id"),
        "source":      raw.get("source", "unknown"),
        "_processed_at": datetime.now(timezone.utc).isoformat(),
        "_layer":      "silver",
    }

    valid, errors = validate_air_quality(cleaned)
    if not valid:
        logger.warning(f"[air_quality] Validation failed {errors} — record dropped")
        return None
    return cleaned


def _clean_weather(raw: dict) -> Optional[dict]:
    """Standardise a weather Bronze record. Returns None if invalid."""
    cleaned = {
        "province":          _normalise_province((raw.get("province") or "").strip()),
        "timestamp":         raw.get("timestamp"),
        "hour_bucket":       _to_hour_bucket(raw.get("timestamp", "")),
        "temperature":       float(raw["temperature"]) if raw.get("temperature") is not None else None,
        "humidity":          float(raw["humidity"]) if raw.get("humidity") is not None else None,
        "weather_condition": raw.get("weather_condition"),
        "message_id":        raw.get("message_id"),
        "source":            raw.get("source", "unknown"),
        "_processed_at":     datetime.now(timezone.utc).isoformat(),
        "_layer":            "silver",
    }

    valid, errors = validate_weather(cleaned)
    if not valid:
        logger.warning(f"[weather] Validation failed {errors} — record dropped")
        return None
    return cleaned


def _clean_flood(raw: dict) -> Optional[dict]:
    """Standardise a flood risk Bronze record. Returns None if invalid."""
    cleaned = {
        "province":          _normalise_province((raw.get("province") or "").strip()),
        "date":              raw.get("date"),
        "flood_risk":        (raw.get("flood_risk") or "").strip(),
        "affected_area_km2": float(raw["affected_area_km2"]) if raw.get("affected_area_km2") is not None else None,
        "water_level_m":     float(raw["water_level_m"]) if raw.get("water_level_m") is not None else None,
        "message_id":        raw.get("message_id"),
        "source":            raw.get("source", "unknown"),
        "_processed_at":     datetime.now(timezone.utc).isoformat(),
        "_layer":            "silver",
    }

    valid, errors = validate_flood(cleaned)
    if not valid:
        logger.warning(f"[flood] Validation failed {errors} — record dropped")
        return None
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Generic Bronze → Silver processor
# ─────────────────────────────────────────────────────────────────────────────

def _process_bronze(
    db,
    bronze_coll_name: str,
    cleaner: Callable[[dict], Optional[dict]],
    since: Optional[str],
) -> List[dict]:
    """
    Read Bronze records ingested after `since`, clean them, and return
    a list of valid Silver records.
    """
    filter_query: Dict = {}
    if since:
        filter_query["_ingested_at"] = {"$gt": since}

    raw_records = list(db[bronze_coll_name].find(filter_query, {"_id": 0}))
    logger.info(f"[{bronze_coll_name}] Read {len(raw_records)} Bronze records")

    cleaned: List[dict] = []
    for raw in raw_records:
        record = cleaner(raw)
        if record:
            cleaned.append(record)

    dropped = len(raw_records) - len(cleaned)
    if dropped:
        logger.warning(f"[{bronze_coll_name}] Dropped {dropped} invalid records")
    logger.info(f"[{bronze_coll_name}] {len(cleaned)} valid records ready for Silver")
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Upsert to Silver
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_silver(db, coll_name: str, records: List[dict], key_fields: List[str]) -> int:
    """
    Bulk-upsert records into a Silver collection using composite key_fields.
    Returns the number of records affected.
    """
    if not records:
        return 0

    ops = [
        UpdateOne(
            filter={f: r[f] for f in key_fields if f in r},
            update={"$set": r},
            upsert=True,
        )
        for r in records
    ]

    result = db[coll_name].bulk_write(ops, ordered=False)
    affected = result.upserted_count + result.modified_count
    logger.info(
        f"[{coll_name}] Upserted {result.upserted_count} new, "
        f"updated {result.modified_count} existing"
    )
    return affected


# ─────────────────────────────────────────────────────────────────────────────
# Join: create silver_environmental_joined
# ─────────────────────────────────────────────────────────────────────────────

def _build_joined_silver(db) -> List[dict]:
    """
    Full outer join of the three Silver sources by (province, date).

    Strategy:
      - For air quality and weather: use today's records, picking the latest
        hour_bucket per province
      - For flood: use today's date record per province
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Air Quality: latest record per province for today ─────────────────────
    aq_by_province: Dict[str, dict] = {}
    for r in (
        db[SILVER_COLLECTIONS["air_quality"]]
        .find({"hour_bucket": {"$regex": f"^{today}"}}, {"_id": 0})
        .sort("hour_bucket", -1)   # newest first
    ):
        if r["province"] not in aq_by_province:
            aq_by_province[r["province"]] = r

    # ── Weather: latest record per province for today ─────────────────────────
    wx_by_province: Dict[str, dict] = {}
    for r in (
        db[SILVER_COLLECTIONS["weather"]]
        .find({"hour_bucket": {"$regex": f"^{today}"}}, {"_id": 0})
        .sort("hour_bucket", -1)
    ):
        if r["province"] not in wx_by_province:
            wx_by_province[r["province"]] = r

    # ── Flood: one record per province per day ────────────────────────────────
    flood_by_province: Dict[str, dict] = {
        r["province"]: r
        for r in db[SILVER_COLLECTIONS["flood"]].find({"date": today}, {"_id": 0})
    }

    # ── Full outer join ───────────────────────────────────────────────────────
    all_provinces = (
        set(aq_by_province) | set(wx_by_province) | set(flood_by_province)
    )

    joined: List[dict] = []
    for province in all_provinces:
        aq    = aq_by_province.get(province, {})
        wx    = wx_by_province.get(province, {})
        flood = flood_by_province.get(province, {})

        joined.append({
            "province":          province,
            "date":              today,
            "hour_bucket":       aq.get("hour_bucket") or wx.get("hour_bucket"),
            "pm25":              aq.get("pm25"),
            "temperature":       wx.get("temperature"),
            "humidity":          wx.get("humidity"),
            "weather_condition": wx.get("weather_condition"),
            "flood_risk":        flood.get("flood_risk"),
            "affected_area_km2": flood.get("affected_area_km2"),
            "water_level_m":     flood.get("water_level_m"),
            "_processed_at":     datetime.now(timezone.utc).isoformat(),
            "_layer":            "silver",
        })

    logger.info(f"[silver_joined] Built {len(joined)} joined province records for {today}")
    return joined


# ─────────────────────────────────────────────────────────────────────────────
# Index setup
# ─────────────────────────────────────────────────────────────────────────────

def _setup_silver_indexes(db) -> None:
    """Create unique compound indexes on Silver collections."""
    db[SILVER_COLLECTIONS["air_quality"]].create_index(
        [("province", ASCENDING), ("hour_bucket", ASCENDING)],
        unique=True, background=True, name="ux_province_hour",
    )
    db[SILVER_COLLECTIONS["weather"]].create_index(
        [("province", ASCENDING), ("hour_bucket", ASCENDING)],
        unique=True, background=True, name="ux_province_hour",
    )
    db[SILVER_COLLECTIONS["flood"]].create_index(
        [("province", ASCENDING), ("date", ASCENDING)],
        unique=True, background=True, name="ux_province_date",
    )
    db[SILVER_COLLECTIONS["joined"]].create_index(
        [("province", ASCENDING), ("date", ASCENDING)],
        unique=True, background=True, name="ux_province_date",
    )
    logger.info("Silver collection indexes verified")


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze_to_silver(sources: list = None) -> None:
    """
    Execute the Bronze → Silver transformation pipeline.

    Args:
        sources: List of sources to process e.g. ["air_quality", "weather", "flood"].
                 Defaults to all three sources when None.
    """
    if sources is None:
        sources = ["air_quality", "weather", "flood"]

    logger.info(f"Bronze → Silver — starting (sources: {sources})")
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DATABASE]

    try:
        _setup_silver_indexes(db)
        since = _load_state()
        if since:
            logger.info(f"Incremental mode — processing Bronze records newer than {since}")

        # ── Air Quality ───────────────────────────────────────────────────────
        if "air_quality" in sources:
            aq_records = _process_bronze(
                db, BRONZE_COLLECTIONS["air_quality"], _clean_air_quality, since
            )
            _upsert_silver(db, SILVER_COLLECTIONS["air_quality"], aq_records,
                           ["province", "hour_bucket"])

        # ── Weather ───────────────────────────────────────────────────────────
        if "weather" in sources:
            wx_records = _process_bronze(
                db, BRONZE_COLLECTIONS["weather"], _clean_weather, since
            )
            _upsert_silver(db, SILVER_COLLECTIONS["weather"], wx_records,
                           ["province", "hour_bucket"])

        # ── Flood ─────────────────────────────────────────────────────────────
        if "flood" in sources:
            fl_records = _process_bronze(
                db, BRONZE_COLLECTIONS["flood"], _clean_flood, since
            )
            _upsert_silver(db, SILVER_COLLECTIONS["flood"], fl_records,
                           ["province", "date"])

        # ── Join all three sources ────────────────────────────────────────────
        joined = _build_joined_silver(db)
        _upsert_silver(db, SILVER_COLLECTIONS["joined"], joined,
                       ["province", "date"])

        _save_state()
        logger.info("Bronze → Silver — completed successfully")

    except Exception as exc:
        logger.error(f"Bronze → Silver — failed: {exc}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    run_bronze_to_silver()
