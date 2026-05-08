"""
main.py – Notification Service

WHY this service exists:
  It listens on the `transaction.flagged` Kafka topic and
  sends alerts for high-risk transactions.

  In a real system, this would integrate with:
  - Email (SendGrid, SES)
  - SMS (Twilio)
  - Slack/PagerDuty webhooks

  For now, we log structured alerts and expose them via API.
  The point is the PATTERN: notification is completely
  decoupled — plugging in real email is a 20-line change.
"""
import json
import logging
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import Response
from kafka import KafkaConsumer
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", "notification")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

# In-memory circular buffer of recent alerts (max 200)
# In prod you'd persist these to a DB or push to a queue
alert_log: deque = deque(maxlen=200)

ALERTS_SENT = Counter("notifications_sent_total", "Total alert notifications sent")


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=30))
def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        "transaction.flagged",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="notification-service",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def consume_loop():
    consumer = create_consumer()
    logger.info("Notification service: Kafka consumer started.")

    for message in consumer:
        txn = message.value
        alert = {
            "transaction_id": txn["id"],
            "user_id": txn["user_id"],
            "amount": txn["amount"],
            "currency": txn.get("currency", "USD"),
            "risk_level": txn.get("risk_level", "HIGH"),
            "alerted_at": datetime.utcnow().isoformat(),
            "message": f"⚠️  HIGH RISK transaction detected for user {txn['user_id']}: ${txn['amount']:.2f}",
        }
        alert_log.append(alert)
        ALERTS_SENT.inc()

        # Log prominently — in prod, replace with actual notification call
        logger.warning(f"ALERT: {alert['message']} | txn_id={alert['transaction_id']}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    yield


app = FastAPI(title="ChaosMend Notification Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/alerts")
def list_alerts(limit: int = 50):
    """Return recent high-risk transaction alerts."""
    alerts = list(alert_log)[-limit:]
    return {"count": len(alerts), "alerts": list(reversed(alerts))}
