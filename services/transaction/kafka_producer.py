"""
kafka_producer.py – Transaction Service

WHY Kafka for this?
  Instead of the transaction service directly calling the
  risk service (tight coupling), it publishes an event to
  Kafka. The risk service subscribes. This means:
  - Transaction service doesn't know/care who consumes it
  - Risk service can go down without blocking transactions
  - We can add more consumers (audit, analytics) without
    changing the transaction service at all

WHY tenacity/retry?
  Kafka might not be ready right when this service starts.
  We retry with exponential backoff instead of crashing.
"""
import json
import logging
import os
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True
)
def create_producer() -> KafkaProducer:
    """Create Kafka producer with retry logic for startup race condition."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        # Serialize Python dicts → JSON bytes automatically
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Wait for all replicas to acknowledge (reliability > throughput here)
        acks="all",
        retries=3,
    )


# Module-level producer (created once, reused across requests)
_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = create_producer()
    return _producer


def publish_transaction_created(transaction_data: dict):
    """Publish a transaction.created event to Kafka."""
    producer = get_producer()
    producer.send(
        topic="transaction.created",
        value=transaction_data,
        # Partition by user_id: all events for same user go to same partition
        # This guarantees ordering per user (important for fraud detection)
        key=transaction_data["user_id"].encode("utf-8"),
    )
    producer.flush()  # Block until message is actually sent
    logger.info(f"Published transaction.created event: {transaction_data['id']}")
