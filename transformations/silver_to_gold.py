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

def _heat_index(temp: Optional[float], humidity: Optional[float]) -> Optional[float]:
    """
    Simplified Heat Index (feels-like temperature).
    Steadman formula approximation, valid when temp >= 27°C and humidity >= 40%.
    Returns None if inputs are unavailable or conditions not met.
    """
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


def _pm25_category(pm25: Optional[float]) -> str:
    """Human-readable PM2.5 air quality category (aligned with Thai PCD standard)."""
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


def _humidity_level(humidity: Optional[float]) -> str:
    """Classify relative humidity into Low / Normal / High."""
    if humidity is None:
        return "Unknown"
    if humidity < 40:
        return "Low"
    if humidity <= 70:
        return "Normal"
    return "High"


def _weather_pm25_impact(weather_condition: Optional[str]) -> str:
    """
    Estimate weather condition's expected impact on PM2.5 dispersion.
    Rain washes particles → Lower PM2.5 expected.
    Clear/hot days trap particles → Higher PM2.5 expected.
    """
    if not weather_condition:
        return "Unknown"
    cond = weather_condition.lower()
    if any(w in cond for w in ["rain", "drizzle", "thunderstorm", "shower"]):
        return "Lower"   # rain washes PM2.5
    if any(w in cond for w in ["clear", "sunny", "hot"]):
        return "Higher"  # stagnant air traps PM2.5
    if any(w in cond for w in ["cloud", "overcast", "mist", "fog", "haze"]):
        return "Moderate"
    return "Neutral"


def _build_fact_record(joined: dict) -> dict:
    """Transform one silver_joined record into a gold fact record."""
    pm25              = joined.get("pm25")
    temperature       = joined.get("temperature")
    humidity          = joined.get("humidity")
    flood_risk        = joined.get("flood_risk")
    weather_condition = joined.get("weather_condition")
    province          = joined.get("province")

    scores = calculate_risk(pm25, temperature, flood_risk)
    coords = _PROVINCE_COORDS.get(province, {})

    return {
        # ── Dimension foreign keys ────────────────────────────────────────────
        "province": province,
        "date":     joined.get("date"),

        # ── Geo coordinates (for Metabase map) ───────────────────────────────
        "lat": coords.get("lat"),
        "lon": coords.get("lon"),

        # ── Measures ─────────────────────────────────────────────────────────
        "pm25":              pm25,
        "temperature":       temperature,
        "humidity":          humidity,
        "weather_condition":   weather_condition,
        # Detailed description from OpenWeatherMap e.g. "light rain", "broken clouds"
        "weather_description": joined.get("weather_description"),
        "flood_risk":          flood_risk,
        "affected_area_km2": joined.get("affected_area_km2"),
        "water_level_m":     joined.get("water_level_m"),

        # ── Risk Scoring ──────────────────────────────────────────────────────
        "pm25_score":  scores["pm25_score"],
        "temp_score":  scores["temp_score"],
        "flood_score": scores["flood_score"],
        "risk_score":  scores["risk_score"],
        "risk_level":  scores["risk_level"],

        # ── Multi-dimensional Analysis ────────────────────────────────────────
        # Human-readable PM2.5 quality label (Good / Moderate / Unhealthy …)
        "pm25_category":          _pm25_category(pm25),
        # Feels-like temperature using Heat Index formula (temp + humidity)
        "heat_index":             _heat_index(temperature, humidity),
        # Humidity classification (Low / Normal / High)
        "humidity_level":         _humidity_level(humidity),
        # Expected effect of weather on PM2.5 (Lower / Higher / Moderate / Neutral)
        "weather_pm25_impact":    _weather_pm25_impact(weather_condition),
        # Is it raining? (boolean for easy filter in Metabase)
        "is_rainy":               weather_condition is not None and any(
            w in (weather_condition or "").lower()
            for w in ["rain", "drizzle", "thunderstorm", "shower"]
        ),

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
