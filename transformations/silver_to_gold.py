"""
transformations/silver_to_gold.py
───────────────────────────────────
Silver → Gold transformation layer.

Responsibilities:
  1. Read records from silver_environmental_joined
  2. Apply the Risk Scoring Model (PM2.5 × 0.5 + Temp × 0.2 + Flood × 0.3)
  3. Build / refresh dimension tables  (dim_location, dim_time)
  4. Upsert into the fact table        (fact_environmental)

Star Schema (Gold):
  ┌──────────────┐        ┌──────────────────────┐
  │ dim_location │◄───────│  fact_environmental  │
  └──────────────┘        │  (province, date)    │
  ┌──────────────┐◄───────│  pm25, temperature,  │
  │   dim_time   │        │  flood_risk,         │
  └──────────────┘        │  risk_score …        │
                          └──────────────────────┘
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.operations import UpdateOne

from config.settings import (
    GOLD_COLLECTIONS,
    MONGODB_DATABASE,
    MONGODB_URI,
    PROVINCES,
    SILVER_COLLECTIONS,
)
from utils.logger import get_logger

logger = get_logger("silver_to_gold")

# Province → coordinates lookup (for embedding into fact records)
_PROVINCE_COORDS: Dict[str, Dict[str, float]] = {
    p["name"]: {"lat": p["lat"], "lon": p["lon"]} for p in PROVINCES
}


# ─────────────────────────────────────────────────────────────────────────────
# RISK SCORING MODEL
# ─────────────────────────────────────────────────────────────────────────────

def _pm25_score(pm25: Optional[float]) -> int:
    """
    PM2.5 Score table:
      0 – 25   →  10  (Good)
      26 – 50  →  30
      51 – 100 →  60
      101 – 150→  80
      151+     → 100  (Very Dangerous)
    """
    if pm25 is None:
        return 0
    if pm25 <= 25:
        return 10
    if pm25 <= 50:
        return 30
    if pm25 <= 100:
        return 60
    if pm25 <= 150:
        return 80
    return 100


def _temp_score(temp: Optional[float]) -> int:
    """
    Temperature Score table:
      < 20 or > 40  →  80   (extreme)
      20 – 25       →  20   (comfortable)
      26 – 30       →  40
      31 – 35       →  60
      36 – 40       →  75
    """
    if temp is None:
        return 0
    if temp < 20 or temp > 40:
        return 80
    if temp <= 25:
        return 20
    if temp <= 30:
        return 40
    if temp <= 35:
        return 60
    return 75   # 36 – 40


def _flood_score(flood_risk: Optional[str]) -> int:
    """
    Flood Risk Score table:
      Low       →  20
      Medium    →  50
      High      →  80
      Very High → 100
    """
    _map = {"Low": 20, "Medium": 50, "High": 80, "Very High": 100}
    return _map.get(flood_risk or "", 0)


def _risk_level(score: float) -> str:
    """
    Composite Risk Level:
       0 – 30  →  Low
      31 – 60  →  Moderate
      61 – 80  →  High
      81 – 100 →  Critical
    """
    if score <= 30:
        return "Low"
    if score <= 60:
        return "Moderate"
    if score <= 80:
        return "High"
    return "Critical"


def calculate_risk(
    pm25: Optional[float],
    temperature: Optional[float],
    flood_risk: Optional[str],
) -> Dict:
    """
    Compute all scoring components for a single province-day record.

    Formula:
        risk_score = (pm25_score × 0.5) + (temp_score × 0.2) + (flood_score × 0.3)
    """
    p_score = _pm25_score(pm25)
    t_score = _temp_score(temperature)
    f_score = _flood_score(flood_risk)

    score = round(p_score * 0.5 + t_score * 0.2 + f_score * 0.3, 2)

    return {
        "pm25_score":  p_score,
        "temp_score":  t_score,
        "flood_score": f_score,
        "risk_score":  score,
        "risk_level":  _risk_level(score),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dimension Builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_dim_location(db) -> None:
    """
    Upsert dim_location from the PROVINCES config.
    Idempotent — safe to run multiple times.
    """
    coll = db[GOLD_COLLECTIONS["dim_location"]]
    coll.create_index([("province", ASCENDING)], unique=True, background=True)

    ops = []
    for p in PROVINCES:
        doc = {
            "province":  p["name"],
            "latitude":  p["lat"],
            "longitude": p["lon"],
            "country":   "Thailand",
            "_updated_at": datetime.now(timezone.utc).isoformat(),
        }
        ops.append(UpdateOne({"province": p["name"]}, {"$set": doc}, upsert=True))

    if ops:
        result = coll.bulk_write(ops)
        logger.info(
            f"dim_location — upserted {result.upserted_count}, "
            f"updated {result.modified_count}"
        )


def _build_dim_time(db, date_str: str) -> None:
    """
    Upsert one dim_time record for the given ISO date string.
    Extracts year, month, quarter, week, day-of-week etc.
    """
    coll = db[GOLD_COLLECTIONS["dim_time"]]
    coll.create_index([("date", ASCENDING)], unique=True, background=True)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    doc = {
        "date":         date_str,
        "year":         dt.year,
        "quarter":      (dt.month - 1) // 3 + 1,
        "month":        dt.month,
        "month_name":   dt.strftime("%B"),
        "week_of_year": dt.isocalendar()[1],
        "day":          dt.day,
        "day_of_week":  dt.strftime("%A"),
        "is_weekend":   dt.weekday() >= 5,
        "_updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    coll.update_one({"date": date_str}, {"$set": doc}, upsert=True)


# ─────────────────────────────────────────────────────────────────────────────
# Fact Table Builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_fact_record(joined: dict) -> dict:
    """Transform one silver_joined record into a gold fact record."""
    pm25        = joined.get("pm25")
    temperature = joined.get("temperature")
    flood_risk  = joined.get("flood_risk")
    province    = joined.get("province")

    scores = calculate_risk(pm25, temperature, flood_risk)
    coords = _PROVINCE_COORDS.get(province, {})

    return {
        # ── Dimension foreign keys ────────────────────────────────────────────
        "province": province,         # → dim_location
        "date":     joined.get("date"),       # → dim_time

        # ── Geo coordinates (for Metabase map) ───────────────────────────────
        "lat": coords.get("lat"),
        "lon": coords.get("lon"),

        # ── Measures ─────────────────────────────────────────────────────────
        "pm25":              pm25,
        "temperature":       temperature,
        "humidity":          joined.get("humidity"),
        "weather_condition": joined.get("weather_condition"),
        "flood_risk":        flood_risk,
        "affected_area_km2": joined.get("affected_area_km2"),
        "water_level_m":     joined.get("water_level_m"),

        # ── Risk Scoring ──────────────────────────────────────────────────────
        "pm25_score":  scores["pm25_score"],
        "temp_score":  scores["temp_score"],
        "flood_score": scores["flood_score"],
        "risk_score":  scores["risk_score"],
        "risk_level":  scores["risk_level"],

        # ── Metadata ──────────────────────────────────────────────────────────
        "hour_bucket":   joined.get("hour_bucket"),
        "_processed_at": datetime.now(timezone.utc).isoformat(),
        "_layer":        "gold",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Index Setup
# ─────────────────────────────────────────────────────────────────────────────

def _setup_gold_indexes(db) -> None:
    fact = db[GOLD_COLLECTIONS["fact"]]
    fact.create_index(
        [("province", ASCENDING), ("date", ASCENDING)],
        unique=True, background=True, name="ux_province_date",
    )
    fact.create_index([("risk_score", DESCENDING)], background=True)
    fact.create_index([("risk_level", ASCENDING)],  background=True)
    logger.info("Gold collection indexes verified")


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_silver_to_gold() -> None:
    """Execute the full Silver → Gold transformation pipeline."""
    logger.info("Silver → Gold — starting")
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DATABASE]

    try:
        _setup_gold_indexes(db)

        # ── Build / refresh dimensions ────────────────────────────────────────
        _build_dim_location(db)

        # ── Read all Silver joined records ────────────────────────────────────
        silver_records: List[dict] = list(
            db[SILVER_COLLECTIONS["joined"]].find({}, {"_id": 0})
        )
        logger.info(f"Silver → Gold — processing {len(silver_records)} joined records")

        # ── Build fact records + dim_time entries ─────────────────────────────
        ops: List[UpdateOne] = []
        for record in silver_records:
            date_str = record.get("date")
            if date_str:
                _build_dim_time(db, date_str)

            fact = _build_fact_record(record)
            ops.append(
                UpdateOne(
                    {"province": fact["province"], "date": fact["date"]},
                    {"$set": fact},
                    upsert=True,
                )
            )

        # ── Bulk upsert into fact table ───────────────────────────────────────
        if ops:
            result = db[GOLD_COLLECTIONS["fact"]].bulk_write(ops, ordered=False)
            logger.info(
                f"fact_environmental — upserted {result.upserted_count}, "
                f"updated {result.modified_count}"
            )

        logger.info("Silver → Gold — completed successfully")

    except Exception as exc:
        logger.error(f"Silver → Gold — failed: {exc}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    run_silver_to_gold()
