"""
consumers/bronze_consumer.py
─────────────────────────────
Kafka consumer that writes raw messages to the MongoDB Bronze layer.

Design:
  - Subscribes to ALL three Kafka topics simultaneously
  - Routes each message to its corresponding Bronze collection
  - Idempotency: MongoDB unique index on message_id prevents duplicate writes
  - Graceful shutdown via SIGINT / SIGTERM
  - Periodic stats logging every 100 messages

Bronze layer principle:
  Store raw data AS-IS — no transformation, no validation.
  Transformation happens in the Silver layer.
"""

import json
import signal
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from config.settings import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPICS,
    MONGODB_DATABASE,
    MONGODB_URI,
    BRONZE_COLLECTIONS,
)
from utils.logger import get_logger

logger = get_logger("bronze_consumer")

# ── Graceful shutdown flag ────────────────────────────────────────────────────
_running = True


def _signal_handler(signum, frame) -> None:
    global _running
    logger.info(f"Shutdown signal received ({signum}). Draining consumer …")
    _running = False


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_db():
    client = MongoClient(MONGODB_URI)
    return client, client[MONGODB_DATABASE]


def _setup_indexes(db) -> None:
    """
    Create unique indexes to enforce idempotency (Rule 4: no duplicates).
    background=True means creation does not block reads/writes.
    """
    for source, coll_name in BRONZE_COLLECTIONS.items():
        coll = db[coll_name]
        # Unique index on message_id — rejects duplicate messages at write time
        coll.create_index(
            [("message_id", ASCENDING)],
            unique=True,
            background=True,
            name="ux_message_id",
        )
        # Compound index for incremental processing queries
        coll.create_index(
            [("province", ASCENDING), ("_ingested_at", ASCENDING)],
            background=True,
            name="ix_province_ingested",
        )
    logger.info("Bronze collection indexes verified")


# ─────────────────────────────────────────────────────────────────────────────
# Topic → collection routing
# ─────────────────────────────────────────────────────────────────────────────

# Build a static mapping once at import time
_TOPIC_TO_COLLECTION = {
    KAFKA_TOPICS["air_quality"]: BRONZE_COLLECTIONS["air_quality"],
    KAFKA_TOPICS["weather"]:     BRONZE_COLLECTIONS["weather"],
    KAFKA_TOPICS["flood"]:       BRONZE_COLLECTIONS["flood"],
}

_TOPIC_TO_SOURCE = {
    KAFKA_TOPICS["air_quality"]: "air_quality",
    KAFKA_TOPICS["weather"]:     "weather",
    KAFKA_TOPICS["flood"]:       "flood",
}


# ─────────────────────────────────────────────────────────────────────────────
# Write to Bronze
# ─────────────────────────────────────────────────────────────────────────────

def _write_to_bronze(collection, record: dict, source: str) -> bool:
    """
    Insert a raw record into the Bronze collection.

    Returns True if written, False if skipped (duplicate).
    Metadata fields (_ingested_at, _source, _layer) are added here.
    """
    record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    record["_source"]      = source
    record["_layer"]       = "bronze"

    try:
        collection.insert_one(record)
        return True
    except DuplicateKeyError:
        # Already exists — silently skip (idempotency guarantee)
        logger.debug(f"Duplicate skipped: message_id={record.get('message_id')}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────────────────────────────────────

def run_consumer() -> None:
    """
    Poll Kafka topics in a loop and write raw messages to MongoDB Bronze.
    Runs until a shutdown signal is received.
    """
    logger.info("Bronze Consumer — starting")

    # signal handlers can only be registered from the main thread
    import threading
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT,  _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    mongo_client, db = _get_db()
    _setup_indexes(db)

    # Pre-resolve collection objects (avoid repeated dict lookups in hot path)
    collections = {
        topic: db[coll_name]
        for topic, coll_name in _TOPIC_TO_COLLECTION.items()
    }

    all_topics = list(KAFKA_TOPICS.values())
    logger.info(f"Subscribing to topics: {all_topics}")

    consumer = KafkaConsumer(
        *all_topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_records=200,
        api_version=(3, 9, 0),
    )

    stats = {"total": 0, "written": 0, "duplicates": 0, "errors": 0}

    try:
        while _running:
            # Poll with a 1-second timeout so we can check _running frequently
            batch = consumer.poll(timeout_ms=1_000)

            for topic_partition, messages in batch.items():
                topic      = topic_partition.topic
                collection = collections.get(topic)
                source     = _TOPIC_TO_SOURCE.get(topic, "unknown")

                if collection is None:
                    logger.warning(f"No collection mapped for topic '{topic}' — skipping")
                    continue

                for message in messages:
                    stats["total"] += 1
                    try:
                        written = _write_to_bronze(collection, message.value, source)
                        if written:
                            stats["written"] += 1
                        else:
                            stats["duplicates"] += 1
                    except Exception as exc:
                        stats["errors"] += 1
                        logger.error(
                            f"Error writing message from '{topic}': {exc} "
                            f"| key={message.key}"
                        )

            # Log stats every 100 messages processed
            if stats["total"] > 0 and stats["total"] % 100 == 0:
                logger.info(
                    f"Stats — total={stats['total']} written={stats['written']} "
                    f"duplicates={stats['duplicates']} errors={stats['errors']}"
                )

    finally:
        consumer.close()
        mongo_client.close()
        logger.info(f"Bronze Consumer stopped. Final stats: {stats}")


if __name__ == "__main__":
    run_consumer()
