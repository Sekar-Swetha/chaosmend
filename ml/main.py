"""
main.py – ML Anomaly Detector Service

Background detection loop polls every POLL_INTERVAL seconds:
  1. Fetch metrics from analytics + Prometheus
  2. Run Isolation Forest + Prophet
  3. Store detected anomalies, publish to Kafka

REST endpoints:
  GET  /api/v1/anomalies            – recent anomaly list (used by healing agent)
  GET  /api/v1/anomalies/model-status – model health + feature info
  POST /api/v1/anomalies/detect-now   – immediate detection (skip wait)
  GET  /api/v1/predictions            – prophet forecast for current window
  GET  /api/v1/anomalies/config       – get detection config
  PUT  /api/v1/anomalies/config       – tune sensitivity at runtime
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel

from anomaly_detector import (
    collect_and_detect,
    fetch_timeseries,
    get_config,
    get_model_status,
    get_recent_anomalies,
    update_config,
    _prophet_model,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

# ── Prometheus metrics ─────────────────────────────────────────────────────
ANOMALIES_TOTAL = Counter(
    "anomalies_detected_total",
    "Total anomalies detected",
    ["model", "severity"],
)
DETECTION_CYCLES = Counter(
    "detection_cycles_total",
    "Total detection cycles run",
)
SAMPLES_COLLECTED = Gauge(
    "ml_samples_collected",
    "Number of metric snapshots in history",
)
MODEL_FITTED = Gauge(
    "ml_model_fitted",
    "1 if model is fitted and ready",
    ["model"],
)


# ── Background detection loop ──────────────────────────────────────────────
async def detection_loop():
    logger.info(f"Detection loop started (interval={POLL_INTERVAL}s)")
    while True:
        try:
            anomaly = collect_and_detect()
            DETECTION_CYCLES.inc()

            status = get_model_status()
            SAMPLES_COLLECTED.set(status["isolation_forest"]["samples_collected"])
            MODEL_FITTED.labels("isolation_forest").set(
                1 if status["isolation_forest"]["fitted"] else 0
            )
            MODEL_FITTED.labels("prophet").set(
                1 if status["prophet"]["fitted"] else 0
            )

            if anomaly:
                ANOMALIES_TOTAL.labels(
                    anomaly.get("model", "unknown"),
                    anomaly.get("severity", "UNKNOWN"),
                ).inc()

        except Exception as e:
            logger.error(f"Detection loop error: {e}")

        await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(detection_loop())
    yield
    task.cancel()


app = FastAPI(
    title="ChaosMend Anomaly Detector",
    description="ML anomaly detection: Isolation Forest + Prophet, fed from Prometheus + Analytics",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Standard endpoints ─────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "healthy", "service": "anomaly-detector"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Anomaly endpoints ──────────────────────────────────────────────────────
@app.get("/api/v1/anomalies")
def list_anomalies(limit: int = 50):
    """
    Most recent detected anomalies. Polled by the self-healing agent.
    The healing agent also subscribes to the anomaly.detected Kafka topic
    for real-time reaction.
    """
    anomalies = get_recent_anomalies(limit)
    return {
        "count": len(anomalies),
        "anomalies": anomalies,
    }


@app.get("/api/v1/anomalies/model-status")
def model_status():
    """Model health: fitted? how many samples? which features?"""
    return get_model_status()


@app.post("/api/v1/anomalies/detect-now")
def detect_now():
    """
    Trigger an immediate detection cycle (skip the 30s wait).
    Useful after manually injecting chaos: trigger chaos → call this
    → see if it's detected immediately.
    """
    anomaly = collect_and_detect()
    return {
        "anomaly_detected": anomaly is not None,
        "anomaly": anomaly,
        "model_status": get_model_status(),
    }


@app.get("/api/v1/predictions")
def predictions():
    """
    Get Prophet's current forecast for transaction count.

    Returns:
      - forecast for next 12 time periods (5-min buckets = 1 hour ahead)
      - yhat, yhat_lower, yhat_upper per period
      - whether the model is fitted

    Useful for Grafana: overlay the forecast on actual transaction counts
    to visually see when the system deviates from expected behaviour.
    """
    import anomaly_detector as det
    if det._prophet_model is None:
        status = get_model_status()
        return {
            "prophet_fitted": False,
            "message": (
                f"Prophet not yet fitted. Need timeseries data. "
                f"Current training points: {status['prophet']['training_points']}"
            ),
        }
    try:
        future   = det._prophet_model.make_future_dataframe(periods=12, freq="5min")
        forecast = det._prophet_model.predict(future)
        last_12  = forecast.tail(12)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        return {
            "prophet_fitted": True,
            "forecast": [
                {
                    "timestamp": str(row["ds"]),
                    "expected": round(float(row["yhat"]), 1),
                    "lower_bound": round(float(row["yhat_lower"]), 1),
                    "upper_bound": round(float(row["yhat_upper"]), 1),
                }
                for _, row in last_12.iterrows()
            ],
        }
    except Exception as e:
        return {"prophet_fitted": True, "error": str(e)}


# ── Config endpoints ───────────────────────────────────────────────────────
class DetectorConfig(BaseModel):
    contamination: Optional[float] = None
    prophet_ci_multiplier: Optional[float] = None
    min_samples_to_fit: Optional[int] = None


@app.get("/api/v1/anomalies/config")
def get_detector_config():
    """
    Return current detection sensitivity config.

    contamination:         Fraction of samples expected to be anomalies.
                           Lower = more sensitive (more alerts).
                           Range: 0.01 (very sensitive) to 0.2 (relaxed).
    prophet_ci_multiplier: Scale the Prophet confidence interval.
                           >1 = wider CI = less sensitive.
                           <1 = narrower CI = more sensitive.
    """
    return get_config()


@app.put("/api/v1/anomalies/config")
def update_detector_config(config: DetectorConfig):
    """
    Tune detection sensitivity at runtime.

    Changes take effect on the next detection cycle (within POLL_INTERVAL seconds).
    Changing contamination forces an Isolation Forest refit.

    Example — make detection more sensitive:
        PUT /api/v1/anomalies/config
        {"contamination": 0.1, "prophet_ci_multiplier": 0.8}
    """
    updates = {k: v for k, v in config.model_dump().items() if v is not None}
    if not updates:
        return {"message": "No changes provided", "config": get_config()}
    new_config = update_config(updates)
    return {"message": "Config updated", "config": new_config}
