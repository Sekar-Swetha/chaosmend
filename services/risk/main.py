"""
main.py – Risk Service

WHY a separate risk service?
  Risk scoring is CPU-intensive and evolves independently
  of the transaction logic. By making it a Kafka consumer
  running in its own container, we can:
  - Scale it independently (more risk workers during peak fraud)
  - Update the scoring logic without touching the transaction service
  - Replay past messages from Kafka to re-score old transactions

HOW it works:
  1. Background thread runs a Kafka consumer loop
  2. On each transaction.created message, score_transaction() runs
  3. If HIGH risk → update DB + publish transaction.flagged
  4. FastAPI exposes /health and /metrics for observability
"""
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", "risk")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://chaosmend:chaosmend_secret@localhost:5432/chaosmend")
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "1000"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# ── Prometheus Metrics ──
EVENTS_PROCESSED = Counter("risk_events_processed_total", "Kafka events processed")
HIGH_RISK_COUNT = Counter("risk_high_risk_transactions_total", "High-risk transactions detected")
PROCESSING_TIME = Histogram("risk_processing_seconds", "Time to score a transaction")


def score_transaction(amount: float, user_id: str) -> str:
    """
    Simple rule-based risk scoring.

    WHY start simple?
      Rule-based scoring is transparent and debuggable.
      Once we have labelled data from real transactions,
      we can swap in an ML model here without changing
      anything else (Kafka consumer stays the same).

    Rules:
      < $100   → LOW
      $100–$1000 → MEDIUM
      > $1000  → HIGH
    """
    if amount > RISK_THRESHOLD:
        return "HIGH"
    elif amount > RISK_THRESHOLD * 0.1:
        return "MEDIUM"
    return "LOW"


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30))
def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        "transaction.created",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="risk-service",          # Consumer group: if we run multiple risk replicas,
        auto_offset_reset="earliest",      # Kafka distributes partitions across them
        enable_auto_commit=True,
    )


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30))
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def consume_loop():
    """
    Runs in a background thread. Continuously polls Kafka
    for new transaction.created messages.

    WHY a thread instead of async?
      kafka-python's consumer is blocking. Running it in a
      thread lets FastAPI stay responsive for /health and /metrics
      while consuming happens in the background.
    """
    consumer = create_consumer()
    producer = create_producer()
    db = SessionLocal()

    logger.info("Risk service: Kafka consumer started, waiting for messages...")

    for message in consumer:
        start = time.time()
        txn = message.value
        logger.info(f"Received transaction: {txn['id']} amount=${txn['amount']}")

        risk_level = score_transaction(txn["amount"], txn["user_id"])
        logger.info(f"Transaction {txn['id']} scored as {risk_level}")

        # Update risk_level in Postgres
        try:
            db.execute(
                text("UPDATE transactions SET risk_level = :rl, updated_at = NOW() WHERE id = :id"),
                {"rl": risk_level, "id": txn["id"]}
            )
            db.commit()
        except Exception as e:
            logger.error(f"DB update failed: {e}")
            db.rollback()

        # If HIGH risk → publish to transaction.flagged so notification service picks it up
        if risk_level == "HIGH":
            HIGH_RISK_COUNT.inc()
            producer.send("transaction.flagged", value={**txn, "risk_level": risk_level})
            producer.flush()
            logger.warning(f"HIGH RISK transaction flagged: {txn['id']} (${txn['amount']})")

        EVENTS_PROCESSED.inc()
        PROCESSING_TIME.observe(time.time() - start)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start consumer in daemon thread (dies when main process exits)
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    logger.info("Risk service consumer thread started.")
    yield


app = FastAPI(title="ChaosMend Risk Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/risk/score")
def score_endpoint(amount: float, user_id: str = "unknown"):
    """Manual risk scoring endpoint — useful for testing the rules."""
    return {"risk_level": score_transaction(amount, user_id), "amount": amount}
