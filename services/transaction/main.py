"""
main.py – Transaction Service

This is the core FastAPI application. It:
  1. Boots up and creates DB tables (if they don't exist)
  2. Exposes REST endpoints for creating/reading transactions
  3. Publishes Kafka events on every new transaction
  4. Exposes /metrics for Prometheus scraping
  5. Exposes /health for Docker healthchecks

WHY FastAPI?
  - Auto-generates /docs (Swagger UI) with zero code
  - Async-native: handles many concurrent requests efficiently
  - Pydantic validation built in: bad input = automatic 422 error
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Depends, HTTPException, status
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from sqlalchemy.orm import Session

from database import engine, get_db, Base
from models import Transaction
from schemas import TransactionCreate, TransactionResponse
from kafka_producer import publish_transaction_created

# ── Logging ──────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", "transaction")

# ── Prometheus Metrics ────────────────────────
# Counter: monotonically increasing (total requests, total errors)
# Histogram: tracks latency distribution (p50, p95, p99 percentiles)
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"]
)
TRANSACTION_COUNT = Counter(
    "transactions_created_total",
    "Total transactions created"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    WHY lifespan?
    FastAPI's modern way to run startup/shutdown code.
    We create DB tables here (idempotent: does nothing if they exist).
    """
    logger.info("Starting Transaction Service — creating DB tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("DB tables ready.")
    yield
    logger.info("Transaction Service shutting down.")


app = FastAPI(
    title="ChaosMend Transaction Service",
    description="Handles financial transactions and publishes events to Kafka",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Health + Metrics endpoints ────────────────
@app.get("/health", tags=["ops"])
def health():
    """Docker/K8s healthcheck endpoint. Returns 200 if service is up."""
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/metrics", tags=["ops"])
def metrics():
    """
    Prometheus scrapes this endpoint every 15s.
    Returns all metric values in Prometheus text format.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Transaction endpoints ─────────────────────
@app.post(
    "/api/v1/transactions",
    response_model=TransactionResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["transactions"],
)
def create_transaction(
    payload: TransactionCreate,
    db: Session = Depends(get_db),  # Dependency injection: FastAPI provides the session
):
    """
    Create a new transaction.

    Flow:
      1. Validate input (Pydantic does this automatically)
      2. Save to PostgreSQL
      3. Publish transaction.created event to Kafka
      4. Return the created transaction

    The key insight: step 3 is async/decoupled.
    Risk and analytics services will pick up the event
    at their own pace — this endpoint doesn't wait for them.
    """
    start_time = time.time()

    try:
        # Save to DB
        txn = Transaction(
            user_id=payload.user_id,
            amount=payload.amount,
            currency=payload.currency,
            description=payload.description,
        )
        db.add(txn)
        db.commit()
        db.refresh(txn)

        # Publish to Kafka (fire-and-forget from this endpoint's perspective)
        publish_transaction_created({
            "id": txn.id,
            "user_id": txn.user_id,
            "amount": txn.amount,
            "currency": txn.currency,
            "description": txn.description,
            "created_at": txn.created_at.isoformat(),
        })

        TRANSACTION_COUNT.inc()
        REQUEST_COUNT.labels("POST", "/api/v1/transactions", "201").inc()
        return txn

    except Exception as e:
        db.rollback()
        REQUEST_COUNT.labels("POST", "/api/v1/transactions", "500").inc()
        logger.error(f"Failed to create transaction: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        REQUEST_LATENCY.labels("POST", "/api/v1/transactions").observe(time.time() - start_time)


@app.get(
    "/api/v1/transactions",
    response_model=List[TransactionResponse],
    tags=["transactions"],
)
def list_transactions(
    limit: int = 50,
    skip: int = 0,
    db: Session = Depends(get_db),
):
    """List recent transactions (paginated)."""
    start_time = time.time()
    txns = db.query(Transaction).order_by(Transaction.created_at.desc()).offset(skip).limit(limit).all()
    REQUEST_COUNT.labels("GET", "/api/v1/transactions", "200").inc()
    REQUEST_LATENCY.labels("GET", "/api/v1/transactions").observe(time.time() - start_time)
    return txns


@app.get(
    "/api/v1/transactions/{transaction_id}",
    response_model=TransactionResponse,
    tags=["transactions"],
)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """Get a single transaction by ID."""
    txn = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return txn
