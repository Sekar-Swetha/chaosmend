"""
anomaly_detector.py – Multi-source ML anomaly detection

DATA SOURCES
────────────
1. Analytics service   → business metrics (transaction volume, risk patterns)
2. Prometheus HTTP API → system metrics (request rate, error rate, latency p99,
                         container CPU/memory via cAdvisor, Kafka consumer lag)

MODELS
──────
1. Isolation Forest (scikit-learn)
   - Unsupervised outlier detection on a 13-feature vector combining both sources
   - Refitted every 50 samples on the rolling history window
   - Works from day 1 with no labelled data

2. Prophet (Meta)
   - Trained on the transaction timeseries from analytics
   - Predicts expected transaction counts for the current time window
   - Catches temporal anomalies (e.g. 10× traffic at 3am) that IF misses
   - Refitted when 10+ new timeseries points are available

Both models run every detection cycle. If EITHER flags → anomaly.
Each detected anomaly is published to the `anomaly.detected` Kafka topic.

SCORING
───────
All anomaly scores are normalised to [0.0, 1.0]:
  0.0 = perfectly normal
  1.0 = most extreme anomaly seen

CONFIGURATION
─────────────
Tunable at runtime via the REST config endpoint without restarting:
  - contamination     (Isolation Forest sensitivity)
  - prophet_threshold (how far outside forecast CI counts as anomaly)
"""

import json
import logging
import os
import uuid
from collections import deque
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import requests
from kafka import KafkaProducer
from sklearn.ensemble import IsolationForest
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

ANALYTICS_URL          = os.getenv("ANALYTICS_URL", "http://analytics:8004")
PROMETHEUS_URL         = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

# ── Tunable config (modified by REST API without restart) ──────────────────
_config = {
    "contamination": 0.05,          # Isolation Forest: expected anomaly fraction
    "prophet_ci_multiplier": 1.0,   # >1 = less sensitive, <1 = more sensitive
    "min_samples_to_fit": 20,       # Min history before IF is meaningful
}

# ── State ──────────────────────────────────────────────────────────────────
_metric_history: deque = deque(maxlen=2000)   # rolling feature history
_anomalies: deque      = deque(maxlen=500)    # detected anomaly buffer

_iso_forest: Optional[IsolationForest] = None
_prophet_model                         = None
_prophet_last_fit_size: int            = 0

# ── Kafka producer ─────────────────────────────────────────────────────────
_producer: Optional[KafkaProducer] = None


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=15))
def _create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def _get_producer() -> Optional[KafkaProducer]:
    global _producer
    if _producer is None:
        try:
            _producer = _create_producer()
        except Exception as e:
            logger.warning(f"Kafka producer not available: {e}")
    return _producer


# ── Prometheus helpers ─────────────────────────────────────────────────────
def _prom_query(promql: str) -> Optional[float]:
    """
    Execute a PromQL instant query and return the first scalar result.
    Returns None on any error so callers can substitute a safe default.
    """
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        resp.raise_for_status()
        results = resp.json().get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
        return 0.0
    except Exception as e:
        logger.debug(f"Prometheus query failed ({promql[:70]}...): {e}")
        return None


def fetch_prometheus_metrics() -> dict:
    """
    Pull system-level metrics from Prometheus.

    WHY Prometheus instead of direct Docker stats?
      Prometheus already stores the rate/histogram calculations.
      rate() smooths out single-scrape spikes. histogram_quantile()
      gives accurate p99 from bucketed data. Both are impossible to
      compute correctly from a single raw counter snapshot.

    Metrics collected:
      • Per-service request rate, error rate (transaction / risk / notification)
      • Transaction service p99 latency
      • Per-container CPU rate + memory MB (cAdvisor)
      • Kafka consumer lag sum (kafka-exporter)
    """
    m: dict = {}

    for service, job in [
        ("transaction", "transaction-service"),
        ("risk",        "risk-service"),
        ("notification","notification-service"),
    ]:
        rate = _prom_query(f'rate(http_requests_total{{job="{job}"}}[5m])')
        m[f"{service}_req_rate"] = rate if rate is not None else 0.0

        err = _prom_query(
            f'rate(http_requests_total{{job="{job}",status=~"5.."}}[5m]) / '
            f'(rate(http_requests_total{{job="{job}"}}[5m]) + 0.001)'
        )
        m[f"{service}_error_rate"] = err if err is not None else 0.0

    # Transaction p99 latency
    p99 = _prom_query(
        'histogram_quantile(0.99, '
        'rate(http_request_duration_seconds_bucket{job="transaction-service"}[5m]))'
    )
    m["transaction_latency_p99"] = p99 if p99 is not None else 0.0

    # Container CPU + memory via cAdvisor
    # (returns 0 gracefully if cAdvisor not yet providing data)
    for svc in ["transaction", "risk"]:
        cpu = _prom_query(
            f'rate(container_cpu_usage_seconds_total{{name="{svc}"}}[5m])'
        )
        m[f"{svc}_cpu_rate"] = cpu if cpu is not None else 0.0

        mem_bytes = _prom_query(
            f'container_memory_usage_bytes{{name="{svc}"}}'
        )
        m[f"{svc}_memory_mb"] = (mem_bytes / 1024 / 1024) if mem_bytes else 0.0

    # Kafka consumer lag (all consumer groups combined)
    lag = _prom_query("sum(kafka_consumergroup_lag) by ()")
    m["kafka_consumer_lag"] = lag if lag is not None else 0.0

    return m


# ── Analytics helpers ──────────────────────────────────────────────────────
def fetch_analytics_metrics() -> Optional[dict]:
    """Fetch business metrics snapshot from the analytics service."""
    try:
        resp = requests.get(f"{ANALYTICS_URL}/api/v1/metrics/summary", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Analytics fetch failed: {e}")
        return None


def fetch_timeseries() -> Optional[pd.DataFrame]:
    """
    Fetch 24h transaction timeseries from analytics.
    Returns a Prophet-ready DataFrame with columns [ds, y], or None if
    there are fewer than 10 data points (not enough to fit Prophet).
    """
    try:
        resp = requests.get(
            f"{ANALYTICS_URL}/api/v1/metrics/timeseries",
            params={"hours": 24, "interval_minutes": 5},
            timeout=10,
        )
        resp.raise_for_status()
        rows = resp.json().get("timeseries", [])
        if len(rows) < 10:
            return None
        df = pd.DataFrame(rows)
        df["ds"] = pd.to_datetime(df["bucket"])
        df["y"]  = df["count"].astype(float)
        return df[["ds", "y"]].dropna()
    except Exception as e:
        logger.warning(f"Timeseries fetch failed: {e}")
        return None


# ── Feature engineering ────────────────────────────────────────────────────
#
# 13-dimensional feature vector fed to Isolation Forest:
#
#  [0]  total_transactions     — absolute transaction count (business volume)
#  [1]  total_volume           — total $ volume
#  [2]  avg_amount             — average transaction size (fraud signal)
#  [3]  high_risk_ratio        — high_risk / total (scale-invariant fraud rate)
#  [4]  transaction_req_rate   — HTTP requests/s (from Prometheus)
#  [5]  transaction_error_rate — 5xx fraction on transaction service
#  [6]  transaction_latency_p99— p99 response time in seconds
#  [7]  risk_req_rate          — HTTP requests/s on risk service
#  [8]  risk_error_rate        — 5xx fraction on risk service
#  [9]  transaction_cpu_rate   — CPU cores consumed (cAdvisor)
#  [10] transaction_memory_mb  — Memory in MB (cAdvisor)
#  [11] risk_cpu_rate          — CPU cores consumed by risk service
#  [12] kafka_consumer_lag     — Total lag across all consumer groups
#
FEATURE_NAMES = [
    "total_transactions", "total_volume", "avg_amount", "high_risk_ratio",
    "transaction_req_rate", "transaction_error_rate", "transaction_latency_p99",
    "risk_req_rate", "risk_error_rate",
    "transaction_cpu_rate", "transaction_memory_mb", "risk_cpu_rate",
    "kafka_consumer_lag",
]


def build_feature_vector(analytics: dict, prom: dict) -> np.ndarray:
    total     = max(analytics.get("total_transactions", 0), 1)
    high_risk = analytics.get("high_risk_count", 0)

    features = [
        analytics.get("total_transactions", 0),
        analytics.get("total_volume", 0),
        analytics.get("avg_amount", 0),
        high_risk / total,
        prom.get("transaction_req_rate", 0),
        prom.get("transaction_error_rate", 0),
        prom.get("transaction_latency_p99", 0),
        prom.get("risk_req_rate", 0),
        prom.get("risk_error_rate", 0),
        prom.get("transaction_cpu_rate", 0),
        prom.get("transaction_memory_mb", 0),
        prom.get("risk_cpu_rate", 0),
        prom.get("kafka_consumer_lag", 0),
    ]
    return np.array(features, dtype=float).reshape(1, -1)


# ── Score normalisation ────────────────────────────────────────────────────
def _normalise_if_score(raw: float) -> float:
    """
    Map Isolation Forest score_samples output → [0.0, 1.0].

    score_samples semantics (sklearn):
      - Higher (closer to 0)  = inlier  (normal)
      - Lower  (more negative) = outlier (anomalous)
      Typical inlier range: [-0.2, 0.0]
      Typical outlier range: [-0.5, -0.2]

    Mapping: anomaly_score = min(1, max(0, -raw * 2))
      raw = 0.0  → 0.0  (perfectly normal)
      raw = -0.5 → 1.0  (extreme outlier)
    """
    return round(min(1.0, max(0.0, -raw * 2.0)), 3)


# ── Isolation Forest ───────────────────────────────────────────────────────
def _fit_isolation_forest() -> None:
    global _iso_forest
    if len(_metric_history) < _config["min_samples_to_fit"]:
        return
    X = np.vstack([row["features"] for row in _metric_history])
    _iso_forest = IsolationForest(
        n_estimators=100,
        contamination=_config["contamination"],
        random_state=42,
    )
    _iso_forest.fit(X)
    logger.info(f"Isolation Forest refitted on {len(_metric_history)} samples.")


def _check_isolation_forest(features: np.ndarray, analytics: dict) -> Optional[dict]:
    if _iso_forest is None:
        return None

    prediction = _iso_forest.predict(features)[0]   # 1=normal, -1=anomaly
    raw_score  = _iso_forest.score_samples(features)[0]

    if prediction == -1:
        score = _normalise_if_score(raw_score)
        return {
            "model":         "isolation_forest",
            "anomaly_score": score,
            "severity":      "HIGH" if score >= 0.7 else "MEDIUM",
            "raw_score":     float(raw_score),
            "metric_name":   "multi_dimensional",
            "actual_value":  score,
            "expected_value": 0.0,
            "description":   (
                f"Multi-dimensional outlier (IF score {score:.3f}): "
                f"txns={analytics.get('total_transactions')}, "
                f"high_risk={analytics.get('high_risk_count')}"
            ),
        }
    return None


# ── Prophet ────────────────────────────────────────────────────────────────
def _fit_prophet(df: pd.DataFrame) -> None:
    """
    Fit Prophet on the transaction count timeseries.

    Prophet learns:
      - Daily seasonality (busy hours vs quiet hours)
      - Trend (growing/declining traffic over time)
      - Changepoints (sudden step changes in baseline)

    WHY not weekly seasonality? We likely don't have 2+ weeks of data yet.
    WHY changepoint_prior_scale=0.05? Low value = conservative — don't chase
    every small fluctuation as a "changepoint". Better for short histories.
    """
    global _prophet_model, _prophet_last_fit_size
    try:
        from prophet import Prophet   # lazy import — Prophet is slow to import
        model = Prophet(
            interval_width=0.95,
            daily_seasonality=True,
            weekly_seasonality=False,
            changepoint_prior_scale=0.05,
            uncertainty_samples=500,
        )
        model.fit(df)
        _prophet_model        = model
        _prophet_last_fit_size = len(df)
        logger.info(f"Prophet refitted on {len(df)} timeseries points.")
    except Exception as e:
        logger.warning(f"Prophet fit failed: {e}")


def _check_prophet(current_count: float) -> Optional[dict]:
    """
    Predict expected transaction count for now and flag deviations.

    A deviation is flagged when the actual count falls outside the
    95% confidence interval (scaled by prophet_ci_multiplier config).

    Anomaly score = how many CI-widths outside the interval.
    """
    if _prophet_model is None:
        return None
    try:
        future   = _prophet_model.make_future_dataframe(periods=1, freq="5min")
        forecast = _prophet_model.predict(future)
        last     = forecast.iloc[-1]

        yhat       = float(last["yhat"])
        yhat_lower = float(last["yhat_lower"]) * _config["prophet_ci_multiplier"]
        yhat_upper = float(last["yhat_upper"]) * _config["prophet_ci_multiplier"]

        if current_count < yhat_lower or current_count > yhat_upper:
            ci_width  = max(yhat_upper - yhat_lower, 1.0)
            deviation = abs(current_count - yhat)
            score     = round(min(1.0, deviation / ci_width), 3)
            return {
                "model":          "prophet",
                "anomaly_score":  score,
                "severity":       "HIGH" if score >= 0.5 else "MEDIUM",
                "metric_name":    "transaction_count",
                "actual_value":   float(current_count),
                "expected_value": yhat,
                "expected_range": [yhat_lower, yhat_upper],
                "description":    (
                    f"Transaction count {current_count:.0f} outside expected "
                    f"[{yhat_lower:.0f}–{yhat_upper:.0f}] (forecast: {yhat:.0f})"
                ),
            }
    except Exception as e:
        logger.warning(f"Prophet predict failed: {e}")
    return None


# ── Kafka publishing ───────────────────────────────────────────────────────
def _publish_anomaly(anomaly: dict) -> None:
    """
    Publish anomaly event to the `anomaly.detected` Kafka topic.

    The self-healing agent subscribes to this topic so it can react
    immediately instead of waiting for its 30s polling cycle.
    Schema matches the doc's API specification (section 9.1).
    """
    producer = _get_producer()
    if not producer:
        return
    try:
        producer.send("anomaly.detected", value=anomaly)
        producer.flush()
        logger.info(f"Published anomaly {anomaly['anomaly_id']} to Kafka")
    except Exception as e:
        logger.warning(f"Kafka publish failed: {e}")


# ── Main detection cycle ───────────────────────────────────────────────────
def collect_and_detect() -> Optional[dict]:
    """
    One detection cycle (called every POLL_INTERVAL seconds):
      1. Fetch metrics from both sources
      2. Build 13-dimensional feature vector
      3. Append to rolling history; refit models periodically
      4. Run Isolation Forest + Prophet
      5. If either fires → merge, store, publish to Kafka, return
    """
    analytics = fetch_analytics_metrics()
    if not analytics:
        return None

    prom     = fetch_prometheus_metrics()
    features = build_feature_vector(analytics, prom)

    _metric_history.append({
        "features":  features.flatten(),
        "analytics": analytics,
        "prometheus": prom,
        "sampled_at": datetime.utcnow().isoformat(),
    })

    # Refit Isolation Forest every 50 samples (or on first run)
    if len(_metric_history) % 50 == 0 or _iso_forest is None:
        _fit_isolation_forest()

    # Refit Prophet when enough new timeseries points have arrived
    ts_df = fetch_timeseries()
    if ts_df is not None and len(ts_df) >= _prophet_last_fit_size + 10:
        _fit_prophet(ts_df)

    # Run both models
    if_result      = _check_isolation_forest(features, analytics)
    prophet_result = _check_prophet(float(analytics.get("total_transactions", 0)))

    # Choose winning detection (both fired → take higher score)
    detection: Optional[dict] = None
    models_fired: list = []
    if if_result:
        models_fired.append("isolation_forest")
    if prophet_result:
        models_fired.append("prophet")

    if if_result and prophet_result:
        detection = (
            if_result if if_result["anomaly_score"] >= prophet_result["anomaly_score"]
            else prophet_result
        )
    elif if_result:
        detection = if_result
    elif prophet_result:
        detection = prophet_result

    if not detection:
        return None

    # Build the canonical anomaly event (matches doc section 9.1 schema)
    anomaly = {
        "anomaly_id":     f"anom_{uuid.uuid4().hex[:8]}",
        "service_name":   "platform",
        "metric_name":    detection.get("metric_name", "multi_dimensional"),
        "actual_value":   detection.get("actual_value", 0.0),
        "expected_value": detection.get("expected_value", 0.0),
        "anomaly_score":  detection["anomaly_score"],
        "severity":       detection["severity"],
        "model":          detection["model"],
        "models_fired":   models_fired,
        "description":    detection["description"],
        "metrics_snapshot": {
            "analytics":   {k: v for k, v in analytics.items() if k != "risk_distribution"},
            "prometheus":  prom,
        },
        "detected_at": datetime.utcnow().isoformat(),
    }
    # Attach Prophet-specific fields if present
    if "expected_range" in detection:
        anomaly["expected_range"] = detection["expected_range"]

    _anomalies.appendleft(anomaly)
    logger.warning(
        f"ANOMALY | model={anomaly['model']} severity={anomaly['severity']} "
        f"score={anomaly['anomaly_score']:.3f} | {anomaly['description']}"
    )
    _publish_anomaly(anomaly)
    return anomaly


# ── Public accessors ───────────────────────────────────────────────────────
def get_recent_anomalies(limit: int = 50) -> list:
    return list(_anomalies)[:limit]


def get_model_status() -> dict:
    return {
        "isolation_forest": {
            "fitted":             _iso_forest is not None,
            "samples_collected":  len(_metric_history),
            "min_samples_to_fit": _config["min_samples_to_fit"],
            "contamination":      _config["contamination"],
            "feature_count":      len(FEATURE_NAMES),
            "features":           FEATURE_NAMES,
        },
        "prophet": {
            "fitted":          _prophet_model is not None,
            "training_points": _prophet_last_fit_size,
            "ci_multiplier":   _config["prophet_ci_multiplier"],
        },
        "anomalies_detected": len(_anomalies),
        "data_sources":       ["analytics", "prometheus_system_metrics"],
    }


def get_config() -> dict:
    return dict(_config)


def update_config(updates: dict) -> dict:
    """
    Update detection sensitivity at runtime.
    Changing contamination forces an Isolation Forest refit on next cycle.
    """
    global _iso_forest
    _config.update(updates)
    if "contamination" in updates:
        _iso_forest = None   # force refit with new contamination
    logger.info(f"Detector config updated: {_config}")
    return dict(_config)
