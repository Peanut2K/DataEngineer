"""
config/settings.py
──────────────────
Central configuration for the Environmental Data Pipeline.
All settings are loaded from environment variables (via .env file).
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Kafka
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# Topic names for each data source
KAFKA_TOPICS = {
    "air_quality": "air_quality_raw",
    "weather":     "weather_raw",
    "flood":       "flood_raw",
}

KAFKA_GROUP_ID           = "environmental_consumers"
KAFKA_AUTO_OFFSET_RESET  = "earliest"

# ─────────────────────────────────────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────────────────────────────────────
MONGODB_URI      = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "environmental_db")

# ── Bronze Layer (raw, no transformation) ────────────────────────────────────
BRONZE_COLLECTIONS = {
    "air_quality": "bronze_air_quality",
    "weather":     "bronze_weather",
    "flood":       "bronze_flood",
}

# ── Silver Layer (cleaned & joined) ──────────────────────────────────────────
SILVER_COLLECTIONS = {
    "air_quality": "silver_air_quality",
    "weather":     "silver_weather",
    "flood":       "silver_flood",
    "joined":      "silver_environmental_joined",
}

# ── Gold Layer (Star Schema, BI-ready) ───────────────────────────────────────
GOLD_COLLECTIONS = {
    "fact":         "fact_environmental",
    "dim_location": "dim_location",
    "dim_time":     "dim_time",
}

# ─────────────────────────────────────────────────────────────────────────────
# External API Keys (optional — pipeline falls back to mock data if not set)
# ─────────────────────────────────────────────────────────────────────────────
OPENAQ_API_KEY       = os.getenv("OPENAQ_API_KEY", "")
OPENAQ_BASE_URL      = "https://api.openaq.org/v2"
OPENAQ_CUSTOM_URL    = "https://api-openaq-data-en.weerapatserver.com/pm25/fixed"

OPENWEATHER_API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"

# ─────────────────────────────────────────────────────────────────────────────
# Provinces (5 locations — Thailand-based example)
# Each has a name, latitude, and longitude for API lookups
# ─────────────────────────────────────────────────────────────────────────────
PROVINCES = [
    {"name": "Bangkok",   "lat": 13.7563, "lon": 100.5018, "weather_query": "Bangkok"},
    {"name": "ChiangMai", "lat": 18.7883, "lon":  98.9853, "weather_query": "Chiang Mai"},
    {"name": "Phuket",    "lat":  7.8804, "lon":  98.3923, "weather_query": "Phuket"},
    {"name": "KhonKaen",  "lat": 16.4322, "lon": 102.8236, "weather_query": "Khon Kaen"},
    {"name": "Chon Buri", "lat": 12.9236, "lon": 100.8825, "weather_query": "Pattaya"},
]

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR   = os.getenv("LOG_DIR", "logs")

# ─────────────────────────────────────────────────────────────────────────────
# Retry Settings
# ─────────────────────────────────────────────────────────────────────────────
MAX_RETRIES  = 3
RETRY_DELAY  = 5    # seconds between retry attempts

# ─────────────────────────────────────────────────────────────────────────────
# Scheduler Intervals
# ─────────────────────────────────────────────────────────────────────────────
AIR_QUALITY_INTERVAL_HOURS      = 1    # Fetch hourly
WEATHER_INTERVAL_HOURS          = 1    # Fetch hourly
FLOOD_INTERVAL_HOURS            = 24   # Fetch daily
TRANSFORMATION_INTERVAL_MINUTES = 30   # Bronze→Silver→Gold every 30 min
