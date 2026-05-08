"""
main.py – Self-Healing Agent Service

TWO CONCURRENT EVENT SOURCES
──────────────────────────────
1. Kafka consumer  (primary)
   Subscribes to `anomaly.detected`.
   Reacts within seconds — no polling delay.

2. REST poll loop  (fallback, every 30s)
   Queries anomaly-detector /api/v1/anomalies.
   Kicks in if Kafka is slow to start or has a blip.

Both sources are deduplicated by anomaly_id so we never
double-heal the same event.

REST ENDPOINTS
──────────────
GET  /api/v1/healing/history          – recent healing records
GET  /api/v1/healing/strategies       – current Q-table
GET  /api/v1/healing/circuit-breakers – circuit-breaker states
GET  /api/v1/healing/stats            – MTTR, success rate, action counts
POST /api/v1/healing/manual           – trigger healing for a specific service
POST /api/v1/healing/heal-now         – immediate heal from latest anomaly
"""

import asyncio
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from kafka import KafkaConsumer
from prometheus_client import (
    Counter, Gauge, Histogram,
    generate_latest, CONTENT_TYPE_LATEST,
)
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from actions import (
    decide_and_heal,
    get_circuit_breaker_status,
    get_q_table,
    compute_stats,
    healing_history,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ANOMALY_DETECTOR_URL    = os.getenv("ANOMALY_DETECTOR_URL",    "http://anomaly-detector:8006")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
POLL_INTERVAL           = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

# ── Prometheus metrics ─────────────────────────────────────────────────────
HEALS_TOTAL = Counter(
    "healing_actions_total",
    "Total healing actions executed",
    ["action", "state", "severity"],
)
HEAL_SUCCESS = Counter(
    "healing_successes_total",
    "Healing actions confirmed successful",
    ["action"],
)
HEAL_FAILURE = Counter(
    "healing_failures_total",
    "Healing actions that did not resolve the anomaly",
    ["action"],
)
MTTR = Histogram(
    "healing_recovery_time_seconds",
    "Time from healing action to confirmed recovery",
    buckets=[15, 30, 60, 90, 120, 180, 300],
)
CB_OPEN = Gauge(
    "healing_circuit_breaker_open",
    "1 if circuit breaker is currently open",
    ["service"],
)
ANOMALIES_RECEIVED = Counter(
    "healing_anomalies_received_total",
    "Anomaly events received by the healing agent",
    ["source"],   # "kafka" or "rest_poll"
)

# ── Deduplication set ──────────────────────────────────────────────────────
# Keyed on anomaly_id; falls back to detected_at for legacy events.
_handled_ids: set = set()
_handled_ids_lock = threading.Lock()


def _already_handled(anomaly: dict) -> bool:
    key = anomaly.get("anomaly_id") or anomaly.get("detected_at")
    if not key:
        return False
    with _handled_ids_lock:
        if key in _handled_ids:
            return True
        _handled_ids.add(key)
        # Keep the set from growing unbounded
        if len(_handled_ids) > 5000:
            _handled_ids.clear()
        return False


def _process_anomaly(anomaly: dict, source: str) -> None:
    """Process one anomaly event from any source."""
    if _already_handled(anomaly):
        return
    ANOMALIES_RECEIVED.labels(source).inc()
    logger.info(
        f"[{source}] New anomaly {anomaly.get('anomaly_id','?')} "
        f"severity={anomaly.get('severity')} — healing"
    )
    record = decide_and_heal(anomaly)
    HEALS_TOTAL.labels(
        record.get("action", "unknown"),
        record.get("state",  "unknown"),
        anomaly.get("severity", "UNKNOWN"),
    ).inc()

    # Update circuit-breaker gauges
    for svc, info in get_circuit_breaker_status().items():
        CB_OPEN.labels(svc).set(1 if info["open"] else 0)


# ── Kafka consumer (background thread) ────────────────────────────────────
@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=3, max=30))
def _create_consumer() -> KafkaConsumer:
    import json
    return KafkaConsumer(
        "anomaly.detected",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="healing-agent",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",       # Only process new anomalies, not replays
        enable_auto_commit=True,
        consumer_timeout_ms=5000,         # Unblock every 5s so we can check stop flag
    )


def _kafka_consumer_thread(stop_event: threading.Event) -> None:
    """
    Runs in a daemon thread.
    WHY a thread and not asyncio? KafkaConsumer is blocking/synchronous.
    A thread lets us keep FastAPI's async event loop unblocked.
    """
    logger.info("Kafka consumer thread starting…")
    consumer: Optional[KafkaConsumer] = None
    while not stop_event.is_set():
        try:
            if consumer is None:
                consumer = _create_consumer()
                logger.info("Kafka consumer connected to anomaly.detected")

            for msg in consumer:
                if stop_event.is_set():
                    break
                anomaly = msg.value
                _process_anomaly(anomaly, source="kafka")

        except StopIteration:
            # consumer_timeout_ms fired with no messages — normal
            pass
        except Exception as e:
            logger.warning(f"Kafka consumer error: {e} — retrying")
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None

    if consumer:
        consumer.close()
    logger.info("Kafka consumer thread stopped")


# ── REST poll loop (fallback, async) ──────────────────────────────────────
async def _rest_poll_loop() -> None:
    """
    Belt-and-suspenders: poll anomaly-detector REST API every POLL_INTERVAL s.
    Catches anomalies that arrive during Kafka startup or short outages.
    Deduplication prevents double-healing events already handled via Kafka.
    """
    logger.info(f"REST poll loop started (every {POLL_INTERVAL}s)")
    while True:
        try:
            resp = requests.get(
                f"{ANOMALY_DETECTOR_URL}/api/v1/anomalies?limit=10",
                timeout=5,
            )
            resp.raise_for_status()
            for anomaly in resp.json().get("anomalies", []):
                _process_anomaly(anomaly, source="rest_poll")
        except Exception as e:
            logger.debug(f"REST poll error: {e}")
        await asyncio.sleep(POLL_INTERVAL)


# ── Lifespan ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Kafka consumer in a background daemon thread
    stop_event = threading.Event()
    t = threading.Thread(
        target=_kafka_consumer_thread,
        args=(stop_event,),
        daemon=True,
        name="kafka-consumer",
    )
    t.start()

    # Start REST poll loop as asyncio background task
    poll_task = asyncio.create_task(_rest_poll_loop())

    yield

    # Shutdown
    stop_event.set()
    poll_task.cancel()


app = FastAPI(
    title="ChaosMend Self-Healing Agent",
    description="Q-learning + circuit-breaker automated recovery",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Standard endpoints ─────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "healthy", "service": "healing-agent"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Healing endpoints ──────────────────────────────────────────────────────
@app.get("/api/v1/healing/history")
def get_healing_history(limit: int = 50):
    """
    Most recent healing records (most recent first).
    Includes: action taken, Q-value at decision time, target service,
    state classification, and verification result once available.
    """
    recent = healing_history[:limit]
    return {"count": len(recent), "actions": recent}


@app.get("/api/v1/healing/strategies")
def get_strategies():
    """
    Current Q-table — the agent's learned strategy map.

    Rows = anomaly states, columns = actions, values = expected success
    probability [0.0–1.0]. Higher = agent prefers this action for this state.

    Values start as domain-knowledge priors and drift toward empirical
    success rates as healing actions are verified.
    """
    return {
        "q_table": get_q_table(),
        "config": {
            "learning_rate_alpha":   0.2,
            "exploration_epsilon":   0.10,
            "description": (
                "Q(s,a) ← Q(s,a) + α × (reward − Q(s,a)), "
                "reward = +1.0 (success) / -1.0 (failure)"
            ),
        },
    }


@app.get("/api/v1/healing/circuit-breakers")
def get_circuit_breakers():
    """
    Circuit-breaker state for every service the healer has touched.
    open=true means restarts are suppressed until the breaker auto-resets.
    """
    return get_circuit_breaker_status()


@app.get("/api/v1/healing/stats")
def get_stats():
    """
    Aggregate healing performance metrics:
      - success_rate: fraction of verified actions that resolved the anomaly
      - mttr_seconds: mean time to recovery (for successful heals)
      - action_counts: how many times each action has been used
    """
    return compute_stats()


class ManualHealRequest(BaseModel):
    service: str
    action: str
    reason: Optional[str] = "Manual trigger via API"


@app.post("/api/v1/healing/manual")
def manual_heal(req: ManualHealRequest):
    """
    Manually trigger a healing action for a specific service.

    Bypasses Q-learning — useful for operators who already know what to do.
    Still runs the background verifier and updates the Q-table with the result.

    Example:
        POST /api/v1/healing/manual
        {"service": "risk", "action": "restart_container", "reason": "manual rollback"}
    """
    valid_actions = ["restart_container", "scale_up", "rate_limit",
                     "circuit_breaker", "alert"]
    if req.action not in valid_actions:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown action '{req.action}'. Valid: {valid_actions}",
        )

    # Build a synthetic anomaly event
    synthetic_anomaly = {
        "anomaly_id":  f"manual_{req.service}",
        "service_name": req.service,
        "severity":    "MEDIUM",
        "description": req.reason,
        "anomaly_score": 0.5,
        "metrics_snapshot": {},
    }
    from actions import (
        _do_restart, _do_scale_up, _do_rate_limit,
        _do_circuit_breaker, _verify_async,
        _record_restart, _select_action,
    )
    import uuid, time
    from datetime import datetime

    exec_result: dict
    if req.action == "restart_container":
        exec_result = _do_restart(req.service)
        _record_restart(req.service)
    elif req.action == "scale_up":
        exec_result = _do_scale_up(req.service)
    elif req.action == "rate_limit":
        exec_result = _do_rate_limit(req.service)
    elif req.action == "circuit_breaker":
        exec_result = _do_circuit_breaker(req.service)
    else:
        exec_result = {"status": "alerted", "description": req.reason}

    record = {
        "action_id":       f"heal_{uuid.uuid4().hex[:8]}",
        "action":          req.action,
        "target":          req.service,
        "state":           "manual",
        "q_value_before":  None,
        "anomaly_id":      synthetic_anomaly["anomaly_id"],
        "severity":        "MEDIUM",
        "started_at":      time.time(),
        "healed_at":       datetime.utcnow().isoformat(),
        "success":         None,
        "execution_result": exec_result,
    }
    healing_history.insert(0, record)
    _verify_async(record, synthetic_anomaly)

    return {"message": "Manual healing action triggered", "action": record}


@app.post("/api/v1/healing/heal-now")
def heal_now():
    """
    Fetch the most recent anomaly and immediately run the healing pipeline.
    Useful for demos: inject chaos → POST /heal-now → watch Q-learning decide.
    """
    try:
        resp = requests.get(
            f"{ANOMALY_DETECTOR_URL}/api/v1/anomalies?limit=1",
            timeout=5,
        )
        anomalies = resp.json().get("anomalies", [])
    except Exception as e:
        return {"error": str(e)}

    if not anomalies:
        return {"message": "No anomalies detected — nothing to heal"}

    anomaly = anomalies[0]
    # Force re-process even if already handled
    key = anomaly.get("anomaly_id") or anomaly.get("detected_at")
    with _handled_ids_lock:
        _handled_ids.discard(key)

    record = decide_and_heal(anomaly)
    return {"message": "Healing action triggered", "action": record}
