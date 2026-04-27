"""
setup_metabase.py
─────────────────
Auto-configures Metabase after it starts:
  1. Completes initial setup (creates admin user)
  2. Connects to MongoDB (environmental_db)
  3. Creates 4-section Environmental Dashboard:
       ┌────────────────────┬────────────────────┐
       │  PM2.5 Bubble Map  │   PM2.5 Table      │
       ├────────────────────┼────────────────────┤
       │  Temperature Map   │   Temperature Table│
       ├────────────────────┼────────────────────┤
       │  Flood Risk Map    │   Flood Risk Table │
       ├────────────────────┼────────────────────┤
       │  Overall Risk Map  │   Overall Risk Tbl │
       └────────────────────┴────────────────────┘

Usage:
    python setup_metabase.py
"""

import json
import os
import sys
import time

import requests

# ─────────────────────────────────────────────────────────────────────────────
# Config — all overridable via environment variables
# ─────────────────────────────────────────────────────────────────────────────
METABASE_URL    = os.getenv("METABASE_URL",   "http://127.0.0.1:3000")
ADMIN_EMAIL     = "admin@dataengineer.local"
ADMIN_PASSWORD  = "Admin1234!"
ADMIN_FIRST     = "Data"
ADMIN_LAST      = "Engineer"
SITE_NAME       = "Environmental Dashboard"

MONGO_HOST      = os.getenv("MONGODB_HOST", "mongodb")
MONGO_PORT      = 27017
MONGO_DB        = "environmental_db"
MONGO_USER      = "admin"
MONGO_PASS      = "password"

# GeoJSON choropleth map
GEOJSON_INTERNAL_URL = "http://geojson-server/thailand_all.geojson"
CUSTOM_MAP_KEY       = "thailand_all"
DIMENSION_COLUMN     = "Province"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _h(token: str) -> dict:
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}


def _wait_for_metabase(timeout: int = 300) -> bool:
    """Poll /api/health until Metabase reports status=ok."""
    print(f"⏳ Waiting for Metabase at {METABASE_URL} …", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.ok and r.json().get("status") == "ok":
                print("✅ Metabase is ready", flush=True)
                return True
        except Exception:
            pass
        print("   … still starting", flush=True)
        time.sleep(10)
    return False


def _get_setup_token() -> str:
    props = requests.get(f"{METABASE_URL}/api/session/properties").json()
    token = props.get("setup-token")
    if not token:
        raise RuntimeError("Setup token not found — Metabase may already be configured")
    return token


def _complete_setup(setup_token: str) -> None:
    payload = {
        "token": setup_token,
        "prefs": {
            "site_name":     SITE_NAME,
            "site_locale":   "en",
            "allow_tracking": False,
        },
        "database": None,
        "invite":   None,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name":  ADMIN_LAST,
            "email":      ADMIN_EMAIL,
            "password":   ADMIN_PASSWORD,
            "site_name":  SITE_NAME,
        },
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Setup failed: {r.status_code} {r.text}")
    print("✅ Admin account created", flush=True)


def _login() -> str:
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    r.raise_for_status()
    token = r.json()["id"]
    print("✅ Logged in", flush=True)
    return token


def _add_mongodb(token: str) -> int:
    payload = {
        "engine": "mongo",
        "name":   "Environmental DB",
        "details": {
            "host":    MONGO_HOST,
            "port":    MONGO_PORT,
            "dbname":  MONGO_DB,
            "user":    MONGO_USER,
            "pass":    MONGO_PASS,
            "authdb":  "admin",
            "ssl":     False,
        },
    }
    r = requests.post(f"{METABASE_URL}/api/database", json=payload, headers=_h(token))
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"✅ MongoDB added (db_id={db_id})", flush=True)
    return db_id


def _wait_for_sync(token: str, db_id: int, timeout: int = 120) -> None:
    """Wait until Metabase has synced all collections."""
    print("⏳ Waiting for schema sync …", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(
            f"{METABASE_URL}/api/database/{db_id}?include=tables",
            headers=_h(token),
        )
        tables = r.json().get("tables", [])
        if any(t["name"] == "fact_environmental" for t in tables):
            print(f"✅ Sync complete ({len(tables)} collections found)", flush=True)
            return
        time.sleep(10)
    print("⚠️  Sync timeout — continuing anyway", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB Aggregation Queries
# ─────────────────────────────────────────────────────────────────────────────

def _q_map(metric_field: str, metric_label: str) -> str:
    """Latest value per province with lat/lon — for region map."""
    # metric_label must NOT contain '.' (MongoDB field name restriction)
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id":        "$province",
            "Province":   {"$first": "$province"},
            "lat":        {"$first": "$lat"},
            "lon":        {"$first": "$lon"},
            metric_label: {"$first": f"${metric_field}"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Province": 1}},
    ]
    return json.dumps(pipeline)


def _q_table_pm25() -> str:
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id":          "$province",
            "Province":     {"$first": "$province"},
            "PM25_ugm3":    {"$first": "$pm25"},
            "PM25_Score":   {"$first": "$pm25_score"},
            "Risk_Level":   {"$first": "$risk_level"},
            "Last_Updated": {"$first": "$date"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"PM25_ugm3": -1}},
    ]
    return json.dumps(pipeline)


def _q_table_weather() -> str:
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id":          "$province",
            "Province":     {"$first": "$province"},
            "Temp_C":       {"$avg": "$temperature"},
            "Humidity_pct": {"$avg": "$humidity"},
            "Condition":    {"$first": "$weather_condition"},
            "Temp_Score":   {"$first": "$temp_score"},
            "Last_Updated": {"$first": "$date"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Temp_C": -1}},
    ]
    return json.dumps(pipeline)


def _q_table_flood() -> str:
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id":              "$province",
            "Province":         {"$first": "$province"},
            "Flood_Risk":       {"$first": "$flood_risk"},
            "Flood_Score":      {"$first": "$flood_score"},
            "Affected_Area_km2":{"$avg": "$affected_area_km2"},
            "Water_Level_m":    {"$avg": "$water_level_m"},
            "Last_Updated":     {"$first": "$date"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Flood_Score": -1}},
    ]
    return json.dumps(pipeline)


def _q_table_risk() -> str:
    pipeline = [
        {"$sort": {"date": -1}},
        {"$group": {
            "_id":          "$province",
            "Province":     {"$first": "$province"},
            "Risk_Score":   {"$first": "$risk_score"},
            "Risk_Level":   {"$first": "$risk_level"},
            "PM25_Score":   {"$first": "$pm25_score"},
            "Temp_Score":   {"$first": "$temp_score"},
            "Flood_Score":  {"$first": "$flood_score"},
            "Last_Updated": {"$first": "$date"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Risk_Score": -1}},
    ]
    return json.dumps(pipeline)


# ── Multi-dimensional / Correlation queries ───────────────────────────────────

def _q_bar_weather_impact() -> str:
    """Pivoted: Category dimension + Lower/Moderate/Higher/Neutral columns — distinct coloured bars."""
    pipeline = [
        {"$match": {"weather_pm25_impact": {"$ne": None}, "pm25": {"$gt": 0}}},
        {"$group": {
            "_id": None,
            "Category": {"$first": {"$literal": "Avg PM2.5 by Weather Impact"}},
            "Lower":    {"$avg": {"$cond": [{"$eq": ["$weather_pm25_impact", "Lower"]},    "$pm25", None]}},
            "Moderate": {"$avg": {"$cond": [{"$eq": ["$weather_pm25_impact", "Moderate"]}, "$pm25", None]}},
            "Higher":   {"$avg": {"$cond": [{"$eq": ["$weather_pm25_impact", "Higher"]},   "$pm25", None]}},
            "Neutral":  {"$avg": {"$cond": [{"$eq": ["$weather_pm25_impact", "Neutral"]},  "$pm25", None]}},
        }},
        {"$project": {"_id": 0}},
    ]
    return json.dumps(pipeline)


def _q_bar_rainy_pm25() -> str:
    """Pivoted: Category dimension + Non_Rainy / Rainy columns — distinct coloured bars."""
    pipeline = [
        {"$match": {"pm25": {"$gt": 0}}},
        {"$group": {
            "_id": None,
            "Category":  {"$first": {"$literal": "Avg PM2.5 Rainy vs Non-Rainy"}},
            "Non_Rainy": {"$avg": {"$cond": ["$is_rainy", None, "$pm25"]}},
            "Rainy":     {"$avg": {"$cond": ["$is_rainy", "$pm25", None]}},
        }},
        {"$project": {"_id": 0}},
    ]
    return json.dumps(pipeline)


def _q_timeseries_daily() -> str:
    """Daily avg PM2.5 (bar) + avg Humidity (line) — combo chart."""
    pipeline = [
        {"$match": {"pm25": {"$ne": None}, "humidity": {"$ne": None}}},
        {"$group": {
            "_id":      "$date",
            "Date":     {"$first": "$date"},
            "PM25":     {"$avg": "$pm25"},
            "Humidity": {"$avg": "$humidity"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Date": 1}},
    ]
    return json.dumps(pipeline)


def _q_timeseries_by_province() -> str:
    """Daily PM2.5 per province — multi-line trend chart."""
    pipeline = [
        {"$match": {"pm25": {"$ne": None}}},
        {"$group": {
            "_id":      {"date": "$date", "province": "$province"},
            "Date":     {"$first": "$date"},
            "Province": {"$first": "$province"},
            "PM25":     {"$avg": "$pm25"},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Date": 1, "Province": 1}},
    ]
    return json.dumps(pipeline)


def _q_weather_vs_flood() -> str:
    """Weather condition × Flood Risk → Avg Risk Score."""
    pipeline = [
        {"$match": {"weather_condition": {"$ne": None}, "flood_risk": {"$ne": None}}},
        {"$group": {
            "_id": {"weather": "$weather_condition", "flood": "$flood_risk"},
            "Weather":        {"$first": "$weather_condition"},
            "Flood_Risk":     {"$first": "$flood_risk"},
            "Avg_Risk_Score": {"$avg": "$risk_score"},
            "Count":          {"$sum": 1},
        }},
        {"$project": {"_id": 0}},
        {"$sort": {"Weather": 1, "Flood_Risk": 1}},
    ]
    return json.dumps(pipeline)


# ── Hourly queries (from silver_environmental_joined) ─────────────────────────

def _q_hourly_trend() -> str:
    """Hourly avg PM2.5 (bar) + avg Humidity (line) — last 24 data points from Gold."""
    pipeline = [
        {"$match": {"hour_bucket": {"$ne": None}}},
        {"$group": {
            "_id":      "$hour_bucket",
            # Strip year: "2026-04-23 00:00" -> "04-23 00:00"
            "Hour":     {"$first": {"$substrCP": ["$hour_bucket", 5, 11]}},
            "PM25":     {"$avg": "$pm25"},
            "Humidity": {"$avg": "$humidity"},
        }},
        {"$sort": {"_id": -1}},
        {"$limit": 24},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0}},
    ]
    return json.dumps(pipeline)


def _q_hourly_by_province() -> str:
    """Hourly PM2.5 per province — multi-line trend (last 24 unique hours)."""
    pipeline = [
        {"$match": {"hour_bucket": {"$ne": None}}},
        {"$group": {
            "_id":      {"h": "$hour_bucket", "p": "$province"},
            # Strip year: "2026-04-23 00:00" -> "04-23 00:00"
            "Hour":     {"$first": {"$substrCP": ["$hour_bucket", 5, 11]}},
            "Province": {"$first": "$province"},
            "PM25":     {"$avg": "$pm25"},
        }},
        {"$sort": {"_id.h": -1}},
        {"$limit": 120},
        {"$sort": {"_id.h": 1, "_id.p": 1}},
        {"$project": {"_id": 0}},
    ]
    return json.dumps(pipeline)


# ─────────────────────────────────────────────────────────────────────────────
# Card / Question Builders
# ─────────────────────────────────────────────────────────────────────────────

def _map_viz(metric_label: str) -> dict:
    return {
        "map.type":             "pin",
        "map.latitude_column":  "lat",
        "map.longitude_column": "lon",
        "map.metric_column":    metric_label,
        "map.zoom":             5,
        "map.center_latitude":  13.5,
        "map.center_longitude": 100.5,
    }


def _create_card(token: str, db_id: int, name: str, display: str,
                 collection: str, query: str, viz: dict) -> int:
    native_block: dict = {"collection": collection, "query": query}
    payload = {
        "name":    name,
        "display": display,
        "dataset_query": {
            "database": db_id,
            "type":     "native",
            "native":   native_block,
        },
        "visualization_settings": viz,
    }
    r = requests.post(f"{METABASE_URL}/api/card", json=payload, headers=_h(token))
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"   ✅ Card created: '{name}' (id={card_id})", flush=True)
    return card_id


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard Builder
# ─────────────────────────────────────────────────────────────────────────────

def _create_dashboard(token: str) -> int:
    r = requests.post(
        f"{METABASE_URL}/api/dashboard",
        json={
            "name":        DASHBOARD_NAME,
            "description": "Real-time PM2.5 · Weather · Flood · Overall Risk by province",
        },
        headers=_h(token),
    )
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"✅ Dashboard created (id={dash_id})", flush=True)
    return dash_id


def _add_all_cards_to_dashboard(
    token: str,
    dash_id: int,
    placements: list,
) -> None:
    """
    Metabase v0.46+ API: all dashcards are submitted in one PUT request.
    Negative IDs signal new cards to the server.
    """
    dashcards = [
        {
            "id":                     -(i + 1),
            "card_id":                p[0],
            "col":                    p[1],
            "row":                    p[2],
            "size_x":                 p[3],
            "size_y":                 p[4],
            "parameter_mappings":     [],
            "visualization_settings": {},
        }
        for i, p in enumerate(placements)
    ]
    r = requests.put(
        f"{METABASE_URL}/api/dashboard/{dash_id}",
        json={"dashcards": dashcards},
        headers=_h(token),
    )
    r.raise_for_status()
    print(f"✅ Added {len(dashcards)} cards to dashboard", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Choropleth (region heatmap) helpers
# ─────────────────────────────────────────────────────────────────────────────

def _configure_geojson(token: str) -> None:
    """Register the full Thailand 77-province GeoJSON in Metabase settings."""
    payload = {
        "value": {
            CUSTOM_MAP_KEY: {
                "name":        "Thailand 77 Provinces",
                "url":         GEOJSON_INTERNAL_URL,
                "region_key":  "name",
                "region_name": "name",
            }
        }
    }
    r = requests.put(
        f"{METABASE_URL}/api/setting/custom-geojson",
        json=payload,
        headers=_h(token),
    )
    r.raise_for_status()
    print(f"✅ GeoJSON registered → {GEOJSON_INTERNAL_URL}", flush=True)


def _update_card_to_heatmap(token: str, card_id: int, card_name: str, metric_col: str) -> None:
    """Convert a pin-map card to a region (choropleth) map card."""
    card = requests.get(f"{METABASE_URL}/api/card/{card_id}", headers=_h(token)).json()
    new_viz = {
        **card.get("visualization_settings", {}),
        "map.type":      "region",
        "map.region":    CUSTOM_MAP_KEY,
        "map.metric":    metric_col,
        "map.dimension": DIMENSION_COLUMN,
    }
    for old_key in [
        "map.latitude_column", "map.longitude_column",
        "map.zoom", "map.center_latitude", "map.center_longitude",
        "map.metric_column", "map.dimension_column",
    ]:
        new_viz.pop(old_key, None)

    r = requests.put(
        f"{METABASE_URL}/api/card/{card_id}",
        json={"visualization_settings": new_viz},
        headers=_h(token),
    )
    r.raise_for_status()
    print(f"   ✅ '{card_name}' → region heatmap  (metric: {metric_col})", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _dashboard_exists(token: str, name: str) -> bool:
    """Return True if a dashboard with this exact name already exists."""
    r = requests.get(f"{METABASE_URL}/api/dashboard", headers=_h(token))
    r.raise_for_status()
    return any(d.get("name") == name for d in r.json())


DASHBOARD_NAME = "Environmental Risk Dashboard — Thailand"


def main() -> None:
    # 1. Wait for Metabase
    if not _wait_for_metabase():
        print("❌ Metabase did not start in time", flush=True)
        sys.exit(1)

    # 2. First-time setup
    try:
        setup_token = _get_setup_token()
        _complete_setup(setup_token)
    except RuntimeError as e:
        # Already configured — just log in
        print(f"ℹ️  {e} — attempting login", flush=True)

    # 3. Login
    token = _login()

    # ── Idempotency check — skip if dashboard already exists ─────────────────
    if _dashboard_exists(token, DASHBOARD_NAME):
        print(
            f"ℹ️  Dashboard '{DASHBOARD_NAME}' already exists — skipping setup.\n"
            f"   Delete it in Metabase first if you want to re-run setup.",
            flush=True,
        )
        return

    # 4. Add MongoDB
    db_id = _add_mongodb(token)

    # 5. Wait for collection sync
    _wait_for_sync(token, db_id)

    # 6. Create questions (cards)
    print("\n📊 Creating questions …", flush=True)

    # ── PM2.5 ─────────────────────────────────────────────────────────────────
    pm25_map_id = _create_card(
        token, db_id,
        name="PM2.5 — Province Bubble Map",
        display="map",
        collection="fact_environmental",
        query=_q_map("pm25", "PM25_ugm3"),
        viz=_map_viz("PM25_ugm3"),
    )
    pm25_tbl_id = _create_card(
        token, db_id,
        name="PM2.5 — Province Table",
        display="table",
        collection="fact_environmental",
        query=_q_table_pm25(),
        viz={},
    )

    # ── Weather ───────────────────────────────────────────────────────────────
    wx_map_id = _create_card(
        token, db_id,
        name="Temperature — Province Bubble Map",
        display="map",
        collection="fact_environmental",
        query=_q_map("temperature", "Temp_C"),
        viz=_map_viz("Temp_C"),
    )
    wx_tbl_id = _create_card(
        token, db_id,
        name="Weather — Province Table",
        display="table",
        collection="fact_environmental",
        query=_q_table_weather(),
        viz={},
    )

    # ── Flood ─────────────────────────────────────────────────────────────────
    flood_map_id = _create_card(
        token, db_id,
        name="Flood Risk — Province Bubble Map",
        display="map",
        collection="fact_environmental",
        query=_q_map("flood_score", "Flood_Score"),
        viz=_map_viz("Flood_Score"),
    )
    flood_tbl_id = _create_card(
        token, db_id,
        name="Flood Risk — Province Table",
        display="table",
        collection="fact_environmental",
        query=_q_table_flood(),
        viz={},
    )

    # ── Overall Risk ──────────────────────────────────────────────────────────
    risk_map_id = _create_card(
        token, db_id,
        name="Overall Risk — Province Bubble Map",
        display="map",
        collection="fact_environmental",
        query=_q_map("risk_score", "Risk_Score"),
        viz=_map_viz("Risk_Score"),
    )
    risk_tbl_id = _create_card(
        token, db_id,
        name="Overall Risk — Province Table",
        display="table",
        collection="fact_environmental",
        query=_q_table_risk(),
        viz={},
    )

    # ── Multi-dimensional / Correlation ───────────────────────────────────────
    print("\n📊 Creating correlation analysis cards …", flush=True)

    corr_impact_id = _create_card(
        token, db_id,
        name="Weather Impact on PM2.5 — Avg PM2.5 by Weather Type",
        display="bar",
        collection="fact_environmental",
        query=_q_bar_weather_impact(),
        viz={
            "graph.dimensions":        ["Category"],
            "graph.metrics":           ["Lower", "Moderate", "Higher", "Neutral"],
            "graph.y_axis.title_text": "Average PM2.5 (µg/m³)",
            "series_settings": {
                "Lower":    {"color": "#4C9BE8"},
                "Moderate": {"color": "#F9CF48"},
                "Higher":   {"color": "#ED6E6E"},
                "Neutral":  {"color": "#84BB4C"},
            },
        },
    )
    corr_rainy_id = _create_card(
        token, db_id,
        name="Rainy vs Non-Rainy — Avg PM2.5 Comparison",
        display="bar",
        collection="fact_environmental",
        query=_q_bar_rainy_pm25(),
        viz={
            "graph.dimensions":        ["Category"],
            "graph.metrics":           ["Non_Rainy", "Rainy"],
            "graph.y_axis.title_text": "Average PM2.5 (µg/m³)",
            "graph.y_axis.auto_split": False,
            "series_settings": {
                "Non_Rainy": {"color": "#84BB4C", "axis": "left"},
                "Rainy":     {"color": "#4C9BE8", "axis": "left"},
            },
        },
    )

    # ── Time Series ───────────────────────────────────────────────────────────
    print("\n📊 Creating time series cards …", flush=True)

    timeseries_id = _create_card(
        token, db_id,
        name="PM2.5 & Humidity — Daily Trend (All Provinces)",
        display="combo",
        collection="fact_environmental",
        query=_q_timeseries_daily(),
        viz={
            "graph.dimensions": ["Date"],
            "graph.metrics":    ["PM25", "Humidity"],
            "graph.show_goal":  False,
            "series_settings": {
                "PM25":     {"display": "bar",  "axis": "left",  "color": "#F9A96E"},
                "Humidity": {"display": "line", "axis": "right", "color": "#4C9BE8",
                             "line.size": "L", "line.marker_size": 8},
            },
        },
    )
    timeseries_prov_id = _create_card(
        token, db_id,
        name="PM2.5 by Province — Daily Trend",
        display="line",
        collection="fact_environmental",
        query=_q_timeseries_by_province(),
        viz={
            "graph.dimensions":  ["Date", "Province"],
            "graph.metrics":     ["PM25"],
            "graph.y_axis.title_text": "PM2.5 (µg/m³)",
            "line.size":         "L",
            "line.marker_size":  8,
        },
    )

    # ── Weather × Flood Correlation ───────────────────────────────────────────
    print("\n📊 Creating weather × flood correlation card …", flush=True)

    weather_flood_id = _create_card(
        token, db_id,
        name="Weather Condition × Flood Risk — Avg Risk Score",
        display="bar",
        collection="fact_environmental",
        query=_q_weather_vs_flood(),
        viz={
            "graph.dimensions":        ["Weather", "Flood_Risk"],
            "graph.metrics":           ["Avg_Risk_Score"],
            "graph.x_axis.title_text": "Weather Condition",
            "graph.y_axis.title_text": "Average Risk Score",
        },
    )

    # ── Hourly trend (from silver_environmental_joined) ───────────────────────
    print("\n📈 Creating hourly trend cards (Silver) …", flush=True)

    hourly_trend_id = _create_card(
        token, db_id,
        name="PM2.5 & Humidity — Hourly Trend (Last 24h)",
        display="combo",
        collection="fact_environmental",
        query=_q_hourly_trend(),
        viz={
            "graph.dimensions": ["Hour"],
            "graph.metrics":    ["PM25", "Humidity"],
            "graph.show_goal":  False,
            "series_settings": {
                "PM25":     {"display": "bar",  "axis": "left",  "color": "#F9A96E"},
                "Humidity": {"display": "line", "axis": "right", "color": "#4C9BE8",
                             "line.size": "L", "line.marker_size": 6},
            },
        },
    )
    hourly_prov_id = _create_card(
        token, db_id,
        name="PM2.5 by Province — Hourly Trend (Last 24h)",
        display="line",
        collection="fact_environmental",
        query=_q_hourly_by_province(),
        viz={
            "graph.dimensions": ["Hour", "Province"],
            "graph.metrics":    ["PM25"],
            "graph.y_axis.title_text": "PM2.5 (µg/m³)",
            "line.size":        "L",
            "line.marker_size": 6,
        },
    )

    # 7. Create dashboard
    print("\n🗂️  Building dashboard …", flush=True)
    dash_id = _create_dashboard(token)

    # 8. Layout (24-col grid, 4 sections × 9 rows each)
    #    Each section: map on left (col 0–11), table on right (col 12–23)
    #    Sections at rows: 0, 9, 18, 27
    placements = [
        # (card_id, col, row, size_x, size_y)
        # ── PM2.5 ──────────────────────────────────
        (pm25_map_id,  0,  0, 12, 9),
        (pm25_tbl_id, 12,  0, 12, 9),
        # ── Weather ────────────────────────────────
        (wx_map_id,    0,  9, 12, 9),
        (wx_tbl_id,   12,  9, 12, 9),
        # ── Flood Risk ─────────────────────────────
        (flood_map_id,  0, 18, 12, 9),
        (flood_tbl_id, 12, 18, 12, 9),
        # ── Overall Risk ───────────────────────────
        (risk_map_id,  0, 27, 12, 9),
        (risk_tbl_id, 12, 27, 12, 9),
        # ── Correlation (Weather Impact / Rainy) ───
        (corr_impact_id,  0, 36, 12, 9),
        (corr_rainy_id,  12, 36, 12, 9),
        # ── Time Series (Daily) ─────────────────────────────
        (timeseries_id,       0, 45, 24, 9),   # PM2.5 + Humidity daily combo (full width)
        (timeseries_prov_id,  0, 54, 24, 9),   # PM2.5 per province daily line (full width)
        # ── Weather × Flood Correlation ────────────
        (weather_flood_id,    0, 63, 24, 9),   # Weather × Flood risk score (full width)
        # ── Hourly View (Silver — last 72h) ────────
        (hourly_trend_id,     0, 72, 24, 9),   # PM2.5 + Humidity hourly combo (full width)
        (hourly_prov_id,      0, 81, 24, 9),   # PM2.5 per province hourly line (full width)
    ]
    # Cards that have DATE_TAGS template variables and should respond to the filter
    _add_all_cards_to_dashboard(token, dash_id, placements)

    # 9. Convert bubble maps → choropleth heatmaps
    print("\n🗺️  Configuring choropleth maps …", flush=True)
    _configure_geojson(token)
    time.sleep(2)
    for card_id, metric_col, card_name in [
        (pm25_map_id,  "PM25_ugm3",   "PM2.5"),
        (wx_map_id,    "Temp_C",      "Temperature"),
        (flood_map_id, "Flood_Score", "Flood Risk"),
        (risk_map_id,  "Risk_Score",  "Overall Risk"),
    ]:
        _update_card_to_heatmap(token, card_id, card_name, metric_col)

    print(f"""
╔══════════════════════════════════════════════════════╗
║  ✅  Dashboard is ready!                             ║
║                                                      ║
║  Open → http://localhost:3000                        ║
║  Email : {ADMIN_EMAIL:<42}║
║  Pass  : {ADMIN_PASSWORD:<42}║
╚══════════════════════════════════════════════════════╝
""", flush=True)


if __name__ == "__main__":
    main()
