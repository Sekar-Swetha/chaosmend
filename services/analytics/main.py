"""
main.py – Analytics Service

WHY this service?
  The ML anomaly detector needs a clean feed of
  system metrics (transaction rates, error rates,
  risk_level distributions). Rather than letting
  the ML layer query the DB directly (tight coupling),
  the analytics service acts as a dedicated metrics
  API. This means:
  - DB schema changes don't break the ML layer
  - Analytics can aggregate/cache expensive queries
  - We can add Redis caching here later without
    touching the ML code
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Depends
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", "analytics")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://chaosmend:chaosmend_secret@localhost:5432/chaosmend")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

REQUEST_COUNT = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "Latency", ["method", "endpoint"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Analytics service started.")
    yield


app = FastAPI(title="ChaosMend Analytics Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/metrics/summary")
def metrics_summary(window_minutes: int = 60, db: Session = Depends(get_db)):
    """
    Returns aggregated transaction metrics for the last N minutes.
    The anomaly detector polls this every 30s to feed its model.

    Returns:
      - total_transactions: count of transactions in window
      - total_volume: sum of transaction amounts
      - high_risk_count: number flagged as HIGH risk
      - avg_amount: average transaction amount
      - risk_distribution: breakdown by risk level
      - window_minutes: the time window used
      - sampled_at: timestamp of this sample
    """
    start_time = time.time()
    since = datetime.utcnow() - timedelta(minutes=window_minutes)

    try:
        result = db.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COALESCE(SUM(amount), 0) as volume,
                    COALESCE(AVG(amount), 0) as avg_amount,
                    COUNT(*) FILTER (WHERE risk_level = 'HIGH') as high_risk,
                    COUNT(*) FILTER (WHERE risk_level = 'MEDIUM') as medium_risk,
                    COUNT(*) FILTER (WHERE risk_level = 'LOW') as low_risk,
                    COUNT(*) FILTER (WHERE risk_level = 'UNKNOWN') as unknown_risk
                FROM transactions
                WHERE created_at >= :since
            """),
            {"since": since}
        ).fetchone()

        summary = {
            "total_transactions": result.total,
            "total_volume": float(result.volume),
            "avg_amount": float(result.avg_amount),
            "high_risk_count": result.high_risk,
            "risk_distribution": {
                "HIGH": result.high_risk,
                "MEDIUM": result.medium_risk,
                "LOW": result.low_risk,
                "UNKNOWN": result.unknown_risk,
            },
            "window_minutes": window_minutes,
            "sampled_at": datetime.utcnow().isoformat(),
        }
        REQUEST_COUNT.labels("GET", "/api/v1/metrics/summary", "200").inc()
        return summary

    except Exception as e:
        logger.error(f"Analytics query failed: {e}")
        REQUEST_COUNT.labels("GET", "/api/v1/metrics/summary", "500").inc()
        # Return zeros on DB error — anomaly detector treats this as baseline
        return {
            "total_transactions": 0, "total_volume": 0.0, "avg_amount": 0.0,
            "high_risk_count": 0, "risk_distribution": {}, "window_minutes": window_minutes,
            "sampled_at": datetime.utcnow().isoformat(), "error": str(e),
        }
    finally:
        REQUEST_LATENCY.labels("GET", "/api/v1/metrics/summary").observe(time.time() - start_time)


@app.get("/api/v1/metrics/timeseries")
def metrics_timeseries(hours: int = 24, interval_minutes: int = 5, db: Session = Depends(get_db)):
    """
    Returns transaction counts bucketed into time intervals.
    Used by Prophet (forecasting model) to learn traffic patterns.
    """
    since = datetime.utcnow() - timedelta(hours=hours)
    try:
        rows = db.execute(
            text("""
                SELECT
                    DATE_TRUNC('minute', created_at) -
                    (EXTRACT(MINUTE FROM created_at)::int % :interval * INTERVAL '1 minute') as bucket,
                    COUNT(*) as count,
                    COALESCE(AVG(amount), 0) as avg_amount
                FROM transactions
                WHERE created_at >= :since
                GROUP BY bucket
                ORDER BY bucket ASC
            """),
            {"since": since, "interval": interval_minutes}
        ).fetchall()

        return {
            "timeseries": [
                {"bucket": str(r.bucket), "count": r.count, "avg_amount": float(r.avg_amount)}
                for r in rows
            ],
            "hours": hours,
            "interval_minutes": interval_minutes,
        }
    except Exception as e:
        logger.error(f"Timeseries query failed: {e}")
        return {"timeseries": [], "error": str(e)}
