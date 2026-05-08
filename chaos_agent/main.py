"""
main.py – Chaos Agent

Exposes a REST API to trigger controlled failure experiments.
Every experiment is logged to Kafka topic `chaos.events`
so the ML layer can learn what failures look like.

Auto-scheduler: runs chaos experiments automatically on a configurable
interval with a configurable probability, so the system is constantly
exercised without requiring manual triggers.
"""
import asyncio
import json
import logging
import os
import random
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from experiments import (
    experiment_container_kill,
    experiment_cpu_stress,
    experiment_latency_inject,
    experiment_memory_stress,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

# ── Metrics ────────────────────────────────────────────────────────────────
CHAOS_TRIGGERED = Counter(
    "chaos_experiments_triggered_total",
    "Chaos experiments triggered (manual + auto)",
    ["type", "mode"],   # mode = "manual" | "auto"
)
CHAOS_SCHEDULE_ENABLED = Gauge(
    "chaos_schedule_enabled",
    "1 if auto-chaos scheduler is active, 0 otherwise",
)

# ── In-memory event history ────────────────────────────────────────────────
# Stores the last 200 chaos events so GET /api/v1/chaos/events can return them.
_event_history: deque = deque(maxlen=200)


# ── Schedule config ────────────────────────────────────────────────────────
class ChaosScheduleConfig(BaseModel):
    """
    Configuration for the automatic chaos scheduler.

    enabled:           Whether auto-chaos is active.
    interval_seconds:  How often the scheduler fires (default 5 min).
    probability:       Chance [0.0–1.0] that a chaos event is injected
                       each interval. 0.3 = 30% chance per interval.
    target_services:   Pool of services to randomly target.
    experiment_types:  Pool of experiment types to randomly pick from.
    duration_seconds:  How long each auto-experiment runs.
    """
    enabled: bool = False
    interval_seconds: int = 300
    probability: float = 0.3
    target_services: List[str] = ["transaction", "risk", "notification", "analytics"]
    experiment_types: List[str] = ["container_kill", "latency_inject", "memory_stress", "cpu_stress"]
    duration_seconds: int = 30


_schedule_config = ChaosScheduleConfig()


# ── Kafka producer ─────────────────────────────────────────────────────────
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=15))
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


_producer = None


def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = create_producer()
        except Exception as e:
            logger.warning(f"Kafka producer not available: {e}")
    return _producer


# ── Experiment helpers ─────────────────────────────────────────────────────
class ExperimentType(str, Enum):
    container_kill = "container_kill"
    latency_inject = "latency_inject"
    memory_stress  = "memory_stress"
    cpu_stress     = "cpu_stress"


class ChaosRequest(BaseModel):
    experiment_type: ExperimentType
    target_service: str
    duration_seconds: int = 30
    latency_ms: Optional[int] = 500   # only for latency_inject


def _run_experiment(request: ChaosRequest, mode: str = "manual") -> dict:
    """
    Execute a chaos experiment and publish the event to Kafka.
    Returns the result dict (may contain status=error).
    mode: "manual" | "auto"
    """
    if request.experiment_type == ExperimentType.container_kill:
        result = experiment_container_kill(request.target_service, request.duration_seconds)
    elif request.experiment_type == ExperimentType.latency_inject:
        result = experiment_latency_inject(
            request.target_service,
            request.latency_ms or 500,
            request.duration_seconds,
        )
    elif request.experiment_type == ExperimentType.memory_stress:
        result = experiment_memory_stress(request.target_service, request.duration_seconds)
    elif request.experiment_type == ExperimentType.cpu_stress:
        result = experiment_cpu_stress(request.target_service, request.duration_seconds)
    else:
        return {"status": "error", "message": "Unknown experiment type"}

    if result.get("status") == "error":
        return result

    CHAOS_TRIGGERED.labels(request.experiment_type, mode).inc()

    event = {
        **result,
        "mode": mode,
        "triggered_at": datetime.utcnow().isoformat(),
    }

    # Store in local history
    _event_history.appendleft(event)

    # Publish to Kafka so the ML layer correlates failures with metrics
    producer = get_producer()
    if producer:
        try:
            producer.send("chaos.events", value=event)
            producer.flush()
        except Exception as e:
            logger.warning(f"Failed to publish chaos event to Kafka: {e}")

    return event


# ── Auto-scheduler ─────────────────────────────────────────────────────────
async def chaos_scheduler():
    """
    Background task that automatically triggers chaos experiments.

    Every `interval_seconds` it rolls the dice: if random() < probability
    it picks a random service and experiment type from the configured pools
    and fires the experiment.

    This means the system is continuously exercised without human intervention,
    which is the core of chaos engineering — chaos should be *expected*, not
    a one-off manual test.
    """
    logger.info("Chaos scheduler started (initially disabled — enable via PUT /api/v1/chaos/config)")
    while True:
        await asyncio.sleep(_schedule_config.interval_seconds)

        if not _schedule_config.enabled:
            continue

        roll = random.random()
        if roll > _schedule_config.probability:
            logger.info(
                f"CHAOS SCHEDULER: Skipped (roll={roll:.2f} > probability={_schedule_config.probability})"
            )
            continue

        target = random.choice(_schedule_config.target_services)
        exp_type = random.choice(_schedule_config.experiment_types)

        logger.warning(f"CHAOS SCHEDULER: Auto-triggering '{exp_type}' on '{target}'")

        req = ChaosRequest(
            experiment_type=ExperimentType(exp_type),
            target_service=target,
            duration_seconds=_schedule_config.duration_seconds,
        )
        result = _run_experiment(req, mode="auto")
        if result.get("status") == "error":
            logger.warning(f"CHAOS SCHEDULER: Experiment failed — {result.get('message')}")


# ── App lifespan ───────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(chaos_scheduler())
    CHAOS_SCHEDULE_ENABLED.set(0)
    logger.info("Chaos Agent started. Scheduler running (disabled by default).")
    yield


app = FastAPI(
    title="ChaosMend Chaos Agent",
    description="Controlled failure injection for distributed system resilience testing",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Standard endpoints ─────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "healthy", "service": "chaos-agent"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Chaos endpoints ────────────────────────────────────────────────────────
@app.post("/api/v1/chaos/trigger")
def trigger_chaos(request: ChaosRequest):
    """
    Manually trigger a chaos experiment.

    Example:
        POST /api/v1/chaos/trigger
        {
          "experiment_type": "cpu_stress",
          "target_service": "transaction",
          "duration_seconds": 60
        }
    """
    logger.warning(f"CHAOS MANUAL: {request.experiment_type} on '{request.target_service}'")
    result = _run_experiment(request, mode="manual")

    if result.get("status") == "error":
        raise HTTPException(status_code=404, detail=result["message"])

    return {"message": "Chaos experiment started", "details": result}


@app.get("/api/v1/chaos/events")
def list_events(limit: int = 50):
    """
    Return the most recent chaos events (manual + auto).
    limit: max number of events to return (default 50, max 200).
    """
    limit = min(limit, 200)
    return {
        "count": min(len(_event_history), limit),
        "events": list(_event_history)[:limit],
    }


@app.get("/api/v1/chaos/config")
def get_config():
    """Return the current auto-chaos scheduler configuration."""
    return _schedule_config.model_dump()


@app.put("/api/v1/chaos/config")
def update_config(config: ChaosScheduleConfig):
    """
    Update the auto-chaos scheduler configuration.

    Example — enable auto-chaos every 5 min with 40% probability:
        PUT /api/v1/chaos/config
        {
          "enabled": true,
          "interval_seconds": 300,
          "probability": 0.4,
          "target_services": ["transaction", "risk"],
          "experiment_types": ["container_kill", "cpu_stress"],
          "duration_seconds": 30
        }
    """
    global _schedule_config
    _schedule_config = config
    CHAOS_SCHEDULE_ENABLED.set(1 if config.enabled else 0)
    logger.info(
        f"Chaos schedule updated: enabled={config.enabled}, "
        f"interval={config.interval_seconds}s, probability={config.probability}"
    )
    return {"message": "Config updated", "config": _schedule_config.model_dump()}


@app.get("/api/v1/chaos/experiments")
def list_experiments():
    """List available chaos experiment types."""
    return {
        "experiments": [
            {
                "type": "container_kill",
                "description": "Stops a service container for duration_seconds, then restarts it",
                "parameters": {"target_service": "str", "duration_seconds": "int"},
            },
            {
                "type": "latency_inject",
                "description": "Injects network latency via tc netem inside the target container",
                "parameters": {"target_service": "str", "duration_seconds": "int", "latency_ms": "int"},
            },
            {
                "type": "memory_stress",
                "description": "Consumes RAM via stress-ng to simulate a memory leak / OOM scenario",
                "parameters": {"target_service": "str", "duration_seconds": "int"},
            },
            {
                "type": "cpu_stress",
                "description": "Maxes out all CPU cores via stress-ng to simulate a runaway process",
                "parameters": {"target_service": "str", "duration_seconds": "int"},
            },
        ]
    }
