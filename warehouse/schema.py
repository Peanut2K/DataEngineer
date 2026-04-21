"""
warehouse/schema.py
────────────────────
MongoDB schema setup for all three Medallion layers.

Responsibilities:
  - Create collections with JSON Schema validation (warn on violation)
  - Create all necessary indexes
  - Safe to run multiple times (idempotent)

Run once before starting the pipeline:
    python -m warehouse.schema
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid

from config.settings import (
    BRONZE_COLLECTIONS,
    GOLD_COLLECTIONS,
    MONGODB_DATABASE,
    MONGODB_URI,
    SILVER_COLLECTIONS,
)
from utils.logger import get_logger

logger = get_logger("schema_setup")


# ─────────────────────────────────────────────────────────────────────────────
# JSON Schema validators
# ─────────────────────────────────────────────────────────────────────────────

# Bronze: minimal validation — just ensure metadata fields are present
_BRONZE_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["message_id", "_ingested_at", "_layer"],
        "properties": {
            "message_id":   {"bsonType": "string", "description": "Idempotency key"},
            "_ingested_at": {"bsonType": "string", "description": "ISO ingestion timestamp"},
            "_layer":       {"bsonType": "string", "enum": ["bronze"]},
        },
    }
}

# Gold fact table: ensure key analytical fields are present and typed correctly
_FACT_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["province", "date", "risk_level"],
        "properties": {
            "province":    {"bsonType": "string"},
            "date":        {"bsonType": "string", "description": "YYYY-MM-DD"},
            "pm25":        {"bsonType": ["double", "int", "null"]},
            "temperature": {"bsonType": ["double", "int", "null"]},
            "risk_score":  {"bsonType": ["double", "int", "null"]},
            "risk_level": {
                "bsonType": "string",
                "enum": ["Low", "Moderate", "High", "Critical"],
            },
            "_layer": {"bsonType": "string", "enum": ["gold"]},
        },
    }
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _create_collection(db, name: str, validator: dict = None) -> None:
    """Create a collection with optional schema validation (warn mode)."""
    try:
        options = {}
        if validator:
            options["validator"]         = validator
            options["validationAction"]  = "warn"   # Log violations, don't reject
            options["validationLevel"]   = "moderate"
        db.create_collection(name, **options)
        logger.info(f"Created collection: {name}")
    except CollectionInvalid:
        logger.debug(f"Collection already exists: {name}")


# ─────────────────────────────────────────────────────────────────────────────
# Schema setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_schema() -> None:
    """
    Create all collections and indexes for the Medallion architecture.
    Safe to call repeatedly (idempotent).
    """
    client = MongoClient(MONGODB_URI)
    db     = client[MONGODB_DATABASE]

    try:
        # ── Bronze Layer ──────────────────────────────────────────────────────
        logger.info("Setting up Bronze layer …")
        for key, coll_name in BRONZE_COLLECTIONS.items():
            _create_collection(db, coll_name, _BRONZE_VALIDATOR)
            coll = db[coll_name]
            coll.create_index(
                [("message_id", ASCENDING)],
                unique=True, background=True, name="ux_message_id",
            )
            coll.create_index(
                [("province", ASCENDING), ("_ingested_at", ASCENDING)],
                background=True, name="ix_province_ingested",
            )

        # ── Silver Layer ──────────────────────────────────────────────────────
        logger.info("Setting up Silver layer …")
        for key, coll_name in SILVER_COLLECTIONS.items():
            _create_collection(db, coll_name)

        # silver_air_quality & silver_weather: unique on (province, hour_bucket)
        for key in ("air_quality", "weather"):
            db[SILVER_COLLECTIONS[key]].create_index(
                [("province", ASCENDING), ("hour_bucket", ASCENDING)],
                unique=True, background=True, name="ux_province_hour",
            )

        # silver_flood: unique on (province, date)
        db[SILVER_COLLECTIONS["flood"]].create_index(
            [("province", ASCENDING), ("date", ASCENDING)],
            unique=True, background=True, name="ux_province_date",
        )

        # silver_joined: unique on (province, date)
        db[SILVER_COLLECTIONS["joined"]].create_index(
            [("province", ASCENDING), ("date", ASCENDING)],
            unique=True, background=True, name="ux_province_date",
        )

        # ── Gold Layer ────────────────────────────────────────────────────────
        logger.info("Setting up Gold layer …")

        # fact_environmental
        _create_collection(db, GOLD_COLLECTIONS["fact"], _FACT_VALIDATOR)
        fact = db[GOLD_COLLECTIONS["fact"]]
        fact.create_index(
            [("province", ASCENDING), ("date", ASCENDING)],
            unique=True, background=True, name="ux_province_date",
        )
        fact.create_index([("risk_score",  DESCENDING)], background=True)
        fact.create_index([("risk_level",  ASCENDING)],  background=True)
        fact.create_index([("date",        DESCENDING)], background=True)

        # dim_location
        _create_collection(db, GOLD_COLLECTIONS["dim_location"])
        db[GOLD_COLLECTIONS["dim_location"]].create_index(
            [("province", ASCENDING)], unique=True, background=True
        )

        # dim_time
        _create_collection(db, GOLD_COLLECTIONS["dim_time"])
        db[GOLD_COLLECTIONS["dim_time"]].create_index(
            [("date", ASCENDING)], unique=True, background=True
        )

        logger.info("Schema setup completed successfully")

    except Exception as exc:
        logger.error(f"Schema setup failed: {exc}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    setup_schema()
