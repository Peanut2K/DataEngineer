"""
utils/kafka_admin.py
────────────────────
Kafka topic management utilities.
Creates required topics if they do not already exist.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
from utils.logger import get_logger

logger = get_logger("kafka_admin")


def create_topics(
    num_partitions: int = 3,
    replication_factor: int = 1,
) -> None:
    """
    Create Kafka topics for all data sources.
    Silently skips topics that already exist.

    Args:
        num_partitions:      Number of partitions per topic.
        replication_factor:  Replication factor (1 for single-broker dev setup).
    """
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="pipeline-admin",
        api_version=(3, 9, 0),
    )

    topics_to_create = [
        NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        for topic_name in KAFKA_TOPICS.values()
    ]

    try:
        admin.create_topics(new_topics=topics_to_create, validate_only=False)
        logger.info(f"Kafka topics created: {list(KAFKA_TOPICS.values())}")
    except TopicAlreadyExistsError:
        logger.info("Kafka topics already exist — skipping creation")
    except Exception as exc:
        logger.error(f"Failed to create Kafka topics: {exc}")
        raise
    finally:
        admin.close()


def list_topics() -> list:
    """Return a list of all topics currently on the broker."""
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="pipeline-admin",
        api_version=(3, 9, 0),
    )
    try:
        return list(admin.list_topics())
    finally:
        admin.close()


if __name__ == "__main__":
    create_topics()
    print("Topics:", list_topics())
