"""
warehouse/queries.py
─────────────────────
BI-ready aggregation queries against the Gold layer.

All queries target fact_environmental and join dim_location / dim_time
where needed.  Each function returns a plain list of dicts ready for
dashboards, DataFrames, or JSON APIs.

Run the full report:
    python -m warehouse.queries
"""

import json
from typing import List, Optional

from pymongo import MongoClient

from config.settings import GOLD_COLLECTIONS, MONGODB_DATABASE, MONGODB_URI
from utils.logger import get_logger

logger = get_logger("bi_queries")


# ─────────────────────────────────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────────────────────────────────

def _get_db():
    client = MongoClient(MONGODB_URI)
    return client, client[MONGODB_DATABASE]


# ─────────────────────────────────────────────────────────────────────────────
# Query 1 — PM2.5 by Province
# ─────────────────────────────────────────────────────────────────────────────

def query_pm25_by_province(top_n: int = 5) -> List[dict]:
    """
    Latest PM2.5 reading + average per province.
    Sorted by highest PM2.5 first (most polluted on top).
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$match": {"pm25": {"$ne": None}}},
            {"$sort":  {"date": -1}},
            {
                "$group": {
                    "_id":          "$province",
                    "latest_pm25":  {"$first": "$pm25"},
                    "latest_date":  {"$first": "$date"},
                    "avg_pm25":     {"$avg":   "$pm25"},
                    "pm25_score":   {"$first": "$pm25_score"},
                }
            },
            {
                "$project": {
                    "_id":         0,
                    "province":    "$_id",
                    "latest_pm25": {"$round": ["$latest_pm25", 2]},
                    "avg_pm25":    {"$round": ["$avg_pm25",    2]},
                    "pm25_score":  1,
                    "latest_date": 1,
                }
            },
            {"$sort":  {"latest_pm25": -1}},
            {"$limit": top_n},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 2 — Temperature by Province
# ─────────────────────────────────────────────────────────────────────────────

def query_temperature_by_province(top_n: int = 5) -> List[dict]:
    """
    Latest temperature + average per province.
    Sorted by highest temperature first.
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$match": {"temperature": {"$ne": None}}},
            {"$sort":  {"date": -1}},
            {
                "$group": {
                    "_id":               "$province",
                    "latest_temp":       {"$first": "$temperature"},
                    "latest_date":       {"$first": "$date"},
                    "avg_temp":          {"$avg":   "$temperature"},
                    "temp_score":        {"$first": "$temp_score"},
                }
            },
            {
                "$project": {
                    "_id":         0,
                    "province":    "$_id",
                    "latest_temp": {"$round": ["$latest_temp", 1]},
                    "avg_temp":    {"$round": ["$avg_temp",    1]},
                    "temp_score":  1,
                    "latest_date": 1,
                }
            },
            {"$sort":  {"latest_temp": -1}},
            {"$limit": top_n},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 3 — Flood Risk by Province
# ─────────────────────────────────────────────────────────────────────────────

def query_flood_risk_by_province(top_n: int = 5) -> List[dict]:
    """
    Latest flood risk level + score per province.
    Sorted by highest flood score first.
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$match": {"flood_risk": {"$ne": None}}},
            {"$sort":  {"date": -1}},
            {
                "$group": {
                    "_id":         "$province",
                    "flood_risk":  {"$first": "$flood_risk"},
                    "flood_score": {"$first": "$flood_score"},
                    "water_level": {"$first": "$water_level_m"},
                    "latest_date": {"$first": "$date"},
                }
            },
            {
                "$project": {
                    "_id":         0,
                    "province":    "$_id",
                    "flood_risk":  1,
                    "flood_score": 1,
                    "water_level": 1,
                    "latest_date": 1,
                }
            },
            {"$sort":  {"flood_score": -1}},
            {"$limit": top_n},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 4 — Risk Score Dashboard (all provinces)
# ─────────────────────────────────────────────────────────────────────────────

def query_risk_dashboard() -> List[dict]:
    """
    Comprehensive risk dashboard for all provinces.
    Joins with dim_location to include lat/lon for map visualisation.
    Sorted by risk_score descending (highest risk on top).
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$sort": {"date": -1}},
            {
                "$group": {
                    "_id":          "$province",
                    "pm25":         {"$first": "$pm25"},
                    "temperature":  {"$first": "$temperature"},
                    "flood_risk":   {"$first": "$flood_risk"},
                    "pm25_score":   {"$first": "$pm25_score"},
                    "temp_score":   {"$first": "$temp_score"},
                    "flood_score":  {"$first": "$flood_score"},
                    "risk_score":   {"$first": "$risk_score"},
                    "risk_level":   {"$first": "$risk_level"},
                    "latest_date":  {"$first": "$date"},
                }
            },
            # Join with dim_location for geo coordinates
            {
                "$lookup": {
                    "from":         GOLD_COLLECTIONS["dim_location"],
                    "localField":   "_id",
                    "foreignField": "province",
                    "as":           "geo",
                }
            },
            {
                "$project": {
                    "_id":         0,
                    "province":    "$_id",
                    "latest_date": 1,
                    "pm25":        {"$round": ["$pm25",        2]},
                    "temperature": {"$round": ["$temperature", 1]},
                    "flood_risk":  1,
                    "pm25_score":  1,
                    "temp_score":  1,
                    "flood_score": 1,
                    "risk_score":  1,
                    "risk_level":  1,
                    "latitude":    {"$arrayElemAt": ["$geo.latitude",  0]},
                    "longitude":   {"$arrayElemAt": ["$geo.longitude", 0]},
                }
            },
            {"$sort": {"risk_score": -1}},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 5 — Risk Score Trend for one Province
# ─────────────────────────────────────────────────────────────────────────────

def query_risk_trend(province: str, days: int = 7) -> List[dict]:
    """
    Historical risk score trend for a single province over the last N days.
    Useful for time-series charts in a BI dashboard.
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$match": {"province": province}},
            {"$sort":  {"date": -1}},
            {"$limit": days},
            {
                "$project": {
                    "_id":        0,
                    "date":       1,
                    "pm25":       1,
                    "temperature": 1,
                    "flood_risk": 1,
                    "risk_score": 1,
                    "risk_level": 1,
                }
            },
            {"$sort": {"date": 1}},   # oldest → newest for time-series display
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 6 — Critical & High Alerts
# ─────────────────────────────────────────────────────────────────────────────

def query_alerts() -> List[dict]:
    """
    Return provinces currently at 'Critical' or 'High' risk level.
    Used for alerting and notification systems.
    """
    client, db = _get_db()
    try:
        pipeline = [
            {"$sort": {"date": -1}},
            {
                "$group": {
                    "_id":         "$province",
                    "risk_level":  {"$first": "$risk_level"},
                    "risk_score":  {"$first": "$risk_score"},
                    "latest_date": {"$first": "$date"},
                }
            },
            {"$match": {"risk_level": {"$in": ["Critical", "High"]}}},
            {
                "$project": {
                    "_id":         0,
                    "province":    "$_id",
                    "risk_level":  1,
                    "risk_score":  1,
                    "latest_date": 1,
                }
            },
            {"$sort": {"risk_score": -1}},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Query 7 — Monthly Summary (aggregated by month)
# ─────────────────────────────────────────────────────────────────────────────

def query_monthly_summary(province: Optional[str] = None) -> List[dict]:
    """
    Monthly averages grouped by year-month.
    Optionally filtered to one province.
    """
    client, db = _get_db()
    try:
        match_stage = {}
        if province:
            match_stage["province"] = province

        pipeline = [
            *([ {"$match": match_stage}] if match_stage else []),
            # Join with dim_time to get month/year
            {
                "$lookup": {
                    "from":         GOLD_COLLECTIONS["dim_time"],
                    "localField":   "date",
                    "foreignField": "date",
                    "as":           "time_info",
                }
            },
            {
                "$group": {
                    "_id": {
                        "year":  {"$arrayElemAt": ["$time_info.year",  0]},
                        "month": {"$arrayElemAt": ["$time_info.month", 0]},
                        "province": "$province",
                    },
                    "avg_pm25":       {"$avg": "$pm25"},
                    "avg_temp":       {"$avg": "$temperature"},
                    "avg_risk_score": {"$avg": "$risk_score"},
                    "record_count":   {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id":            0,
                    "year":           "$_id.year",
                    "month":          "$_id.month",
                    "province":       "$_id.province",
                    "avg_pm25":       {"$round": ["$avg_pm25",       2]},
                    "avg_temp":       {"$round": ["$avg_temp",       1]},
                    "avg_risk_score": {"$round": ["$avg_risk_score", 2]},
                    "record_count":   1,
                }
            },
            {"$sort": {"year": 1, "month": 1, "province": 1}},
        ]
        return list(db[GOLD_COLLECTIONS["fact"]].aggregate(pipeline))
    finally:
        client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Full dashboard report (CLI entry point)
# ─────────────────────────────────────────────────────────────────────────────

def run_all_queries() -> None:
    """Print a formatted BI report to stdout."""
    separator = "=" * 65

    print(f"\n{separator}")
    print("  ENVIRONMENTAL RISK DASHBOARD — BI REPORT")
    print(separator)

    print("\n[ PM2.5 by Province (highest first) ]")
    for r in query_pm25_by_province():
        print(f"  {r['province']:<12} PM2.5={r['latest_pm25']:>7} µg/m³  "
              f"avg={r['avg_pm25']:>7}  score={r['pm25_score']}")

    print("\n[ Temperature by Province (highest first) ]")
    for r in query_temperature_by_province():
        print(f"  {r['province']:<12} Temp={r['latest_temp']:>6} °C  "
              f"avg={r['avg_temp']:>5}  score={r['temp_score']}")

    print("\n[ Flood Risk by Province (highest score first) ]")
    for r in query_flood_risk_by_province():
        print(f"  {r['province']:<12} {r['flood_risk']:<10}  "
              f"score={r['flood_score']}  water={r.get('water_level', 'N/A')} m")

    print("\n[ Risk Score Dashboard ]")
    print(f"  {'Province':<12} {'PM2.5':>8} {'Temp':>6} {'Flood':<10} "
          f"{'Score':>6}  Level")
    print("  " + "-" * 55)
    for r in query_risk_dashboard():
        print(
            f"  {r['province']:<12} "
            f"{str(r.get('pm25','N/A')):>8} "
            f"{str(r.get('temperature','N/A')):>6} "
            f"{str(r.get('flood_risk','N/A')):<10} "
            f"{r.get('risk_score', 0):>6.1f}  "
            f"{r.get('risk_level','N/A')}"
        )

    print("\n[ Active Alerts (Critical / High) ]")
    alerts = query_alerts()
    if alerts:
        for r in alerts:
            print(f"  !! {r['province']:<12} {r['risk_level']:<10}  score={r['risk_score']}")
    else:
        print("  No active alerts")

    print(f"\n{separator}\n")


if __name__ == "__main__":
    run_all_queries()
