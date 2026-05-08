"""
actions.py – Self-Healing Action Engine

HOW A HEALING CYCLE WORKS
──────────────────────────
1. Anomaly arrives (from Kafka consumer OR REST poll)
2. Classify anomaly → one of 7 named states
3. Circuit-breaker check — if service restarted ≥3 times in 5 min → skip to alert
4. Q-learning selects action (ε-greedy: 10% random exploration)
5. Execute action via Docker SDK
6. Background thread waits 120s then re-runs anomaly detection
7. Verification result → update Q-table  (+1 reward / -1 penalty)
8. Final outcome published to healing.actions Kafka topic

Q-LEARNING
──────────
State space : 7 categories (cpu_high, memory_high, error_rate_high,
              latency_high, risk_high, kafka_lag_high, general)
Action space: 5 actions (restart_container, scale_up, rate_limit,
              circuit_breaker, alert)
Q-table     : state × action → expected success probability [0.0, 1.0]
Seed values : domain-knowledge priors (e.g. memory_high → restart=0.9)
Update rule : Q(s,a) ← Q(s,a) + α × (reward − Q(s,a))
              α = 0.2, reward ∈ {+1.0 success, −1.0 failure}

CIRCUIT BREAKER
───────────────
Per-service sliding window (5 min).
If restarts ≥ 3 in the window → circuit OPEN → route to "alert" instead.
Auto-resets after 10 min of quiet.
Prevents infinite restart loops when the root cause isn't fixable by restart.
"""

import json
import logging
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Optional

import docker
import requests
from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

ANOMALY_DETECTOR_URL    = os.getenv("ANOMALY_DETECTOR_URL",    "http://anomaly-detector:8006")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

# ── Docker client ──────────────────────────────────────────────────────────
try:
    docker_client = docker.from_env()
    logger.info("Docker client initialised")
except Exception:
    try:
        docker_client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
        logger.info("Docker client initialised via explicit socket")
    except Exception as _e:
        docker_client = None
    logger.warning(f"Docker client unavailable: {_e}")

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
            logger.warning(f"Kafka producer unavailable: {e}")
    return _producer


# ── Healing history (most-recent-first) ───────────────────────────────────
healing_history: list = []

# ── Q-Table ────────────────────────────────────────────────────────────────
# Seeded with domain-knowledge priors.
# Values drift toward empirical success rates as actions are verified.
_Q: dict = {
    "cpu_high": {
        "scale_up":          0.80,
        "restart_container": 0.60,
        "rate_limit":        0.40,
        "circuit_breaker":   0.30,
        "alert":             0.20,
    },
    "memory_high": {
        "restart_container": 0.90,
        "scale_up":          0.50,
        "rate_limit":        0.35,
        "circuit_breaker":   0.30,
        "alert":             0.20,
    },
    "error_rate_high": {
        "circuit_breaker":   0.85,
        "restart_container": 0.70,
        "rate_limit":        0.60,
        "scale_up":          0.40,
        "alert":             0.30,
    },
    "latency_high": {
        "scale_up":          0.90,
        "rate_limit":        0.60,
        "restart_container": 0.50,
        "circuit_breaker":   0.35,
        "alert":             0.25,
    },
    "risk_high": {
        "alert":             0.80,
        "rate_limit":        0.75,
        "circuit_breaker":   0.65,
        "restart_container": 0.40,
        "scale_up":          0.30,
    },
    "kafka_lag_high": {
        "scale_up":          0.85,
        "restart_container": 0.55,
        "rate_limit":        0.45,
        "circuit_breaker":   0.30,
        "alert":             0.25,
    },
    "general": {
        "restart_container": 0.60,
        "scale_up":          0.50,
        "rate_limit":        0.45,
        "circuit_breaker":   0.35,
        "alert":             0.30,
    },
}

_ALPHA   = 0.2    # learning rate
_EPSILON = 0.10   # exploration fraction (ε-greedy)

# ── Circuit breaker ────────────────────────────────────────────────────────
_restart_ts: dict = defaultdict(list)   # service → [datetime, ...]
_cb_open: dict    = defaultdict(bool)   # service → is breaker open?
_cb_opened_at: dict = {}                # service → datetime tripped

_CB_WINDOW_MIN  = 5
_CB_MAX_RESTARTS = 3
_CB_RESET_MIN   = 10


def _is_circuit_open(service: str) -> bool:
    """
    True if the circuit breaker for this service is open (too many restarts).
    Auto-resets after CB_RESET_MIN minutes of quiet.
    """
    # Auto-reset check
    if _cb_open[service]:
        opened = _cb_opened_at.get(service)
        if opened and datetime.utcnow() - opened > timedelta(minutes=_CB_RESET_MIN):
            _cb_open[service] = False
            _restart_ts[service].clear()
            logger.info(f"Circuit breaker RESET for '{service}'")

    # Prune old timestamps outside the window
    cutoff = datetime.utcnow() - timedelta(minutes=_CB_WINDOW_MIN)
    _restart_ts[service] = [t for t in _restart_ts[service] if t > cutoff]

    if len(_restart_ts[service]) >= _CB_MAX_RESTARTS and not _cb_open[service]:
        _cb_open[service]    = True
        _cb_opened_at[service] = datetime.utcnow()
        logger.warning(
            f"Circuit breaker OPENED for '{service}' "
            f"({_CB_MAX_RESTARTS}+ restarts in {_CB_WINDOW_MIN}min)"
        )

    return bool(_cb_open[service])


def _record_restart(service: str) -> None:
    _restart_ts[service].append(datetime.utcnow())


# ── State classification ───────────────────────────────────────────────────
def _classify_state(anomaly: dict) -> str:
    """
    Map an anomaly dict to one of 7 Q-table states.

    Priority order (most specific → least):
      1. explicit metric_name field
      2. description text keywords
      3. numeric feature values in metrics_snapshot
      4. fallback → "general"
    """
    metric = anomaly.get("metric_name", "").lower()
    desc   = anomaly.get("description",  "").lower()
    snap   = anomaly.get("metrics_snapshot", {})
    prom   = snap.get("prometheus", {})
    anl    = snap.get("analytics",  {})

    # 1 – explicit metric_name
    if "cpu"        in metric: return "cpu_high"
    if "memory"     in metric: return "memory_high"
    if "error_rate" in metric: return "error_rate_high"
    if "latency"    in metric: return "latency_high"
    if "risk"       in metric: return "risk_high"
    if "lag"        in metric or "kafka" in metric: return "kafka_lag_high"

    # 2 – description keywords
    if "cpu"                       in desc: return "cpu_high"
    if "memory"                    in desc: return "memory_high"
    if "error" in desc or "5xx"    in desc: return "error_rate_high"
    if "latency" in desc or "p99"  in desc: return "latency_high"
    if "risk"                      in desc: return "risk_high"
    if "kafka" in desc or "lag"    in desc: return "kafka_lag_high"

    # 3 – numeric thresholds
    if prom.get("transaction_error_rate", 0)  > 0.05:  return "error_rate_high"
    if prom.get("transaction_latency_p99", 0) > 2.0:   return "latency_high"
    if prom.get("kafka_consumer_lag", 0)       > 1000:  return "kafka_lag_high"
    if prom.get("transaction_cpu_rate", 0)    > 0.80:  return "cpu_high"
    if prom.get("transaction_memory_mb", 0)   > 400:   return "memory_high"

    total = max(anl.get("total_transactions", 1), 1)
    if anl.get("high_risk_count", 0) / total > 0.30:   return "risk_high"

    return "general"


def _pick_target(state: str, anomaly: dict) -> str:
    """
    Pick the Docker container name most likely responsible for this anomaly.
    Falls back to 'transaction' (the platform entry-point) when unsure.
    """
    prom = anomaly.get("metrics_snapshot", {}).get("prometheus", {})

    defaults = {
        "cpu_high":        "transaction",
        "memory_high":     "transaction",
        "error_rate_high": "transaction",
        "latency_high":    "transaction",
        "risk_high":       "risk",
        "kafka_lag_high":  "risk",
        "general":         "transaction",
    }
    target = defaults.get(state, "transaction")

    # Override if risk service has a higher error rate than transaction
    if prom.get("risk_error_rate", 0) > prom.get("transaction_error_rate", 0):
        target = "risk"

    return target


# ── Q-Learning helpers ─────────────────────────────────────────────────────
def _select_action(state: str) -> str:
    """
    ε-greedy selection:
      - 10% of the time: choose a random action (explore)
      - 90% of the time: choose the action with highest Q-value (exploit)

    WHY exploration?
      If we always exploit, we never discover that a currently low-rated
      action might actually work well for a new kind of anomaly.
    """
    actions = _Q.get(state, _Q["general"])
    if random.random() < _EPSILON:
        chosen = random.choice(list(actions.keys()))
        logger.info(f"Q-EXPLORE → {chosen} (state={state})")
    else:
        chosen = max(actions, key=actions.get)
        logger.info(f"Q-EXPLOIT → {chosen} Q={actions[chosen]:.3f} (state={state})")
    return chosen


def _q_update(state: str, action: str, reward: float) -> None:
    """
    Simplified Bellman update (single-step, no discounted future):
      Q(s,a) ← Q(s,a) + α × (reward − Q(s,a))

    No next-state term because healing is a one-shot decision —
    we don't plan multi-step sequences of healing actions.
    """
    if state not in _Q:
        _Q[state] = {a: 0.5 for a in ["scale_up", "restart_container",
                                        "rate_limit", "circuit_breaker", "alert"]}
    old = _Q[state].get(action, 0.5)
    new = round(min(1.0, max(0.0, old + _ALPHA * (reward - old))), 4)
    _Q[state][action] = new
    logger.info(
        f"Q-UPDATE state={state} action={action} "
        f"{old:.4f} → {new:.4f} (reward={reward:+.1f})"
    )


# ── Kafka publisher ────────────────────────────────────────────────────────
def _publish_healing_action(record: dict, anomaly: dict) -> None:
    """
    Publish the final healing outcome to the healing.actions Kafka topic.
    Called from the background verification thread (has success + recovery_time).
    """
    producer = _get_producer()
    if not producer:
        return
    message = {
        "action_id":             record["action_id"],
        "anomaly_id":            anomaly.get("anomaly_id", "unknown"),
        "action_type":           record["action"],
        "target_service":        record["target"],
        "state":                 record["state"],
        "replicas_before":       1,
        "replicas_after":        3 if record["action"] == "scale_up" else 1,
        "success":               record.get("success"),
        "recovery_time_seconds": record.get("recovery_time_seconds"),
        "q_value_before":        record.get("q_value_before"),
        "timestamp":             record.get("healed_at"),
    }
    try:
        producer.send("healing.actions", value=message)
        producer.flush()
        logger.info(f"Published to healing.actions: {record['action_id']}")
    except Exception as e:
        logger.warning(f"Kafka publish failed: {e}")


# ── Docker actions ─────────────────────────────────────────────────────────
def _find_container(service: str):
    if not docker_client:
        return None
    try:
        for c in docker_client.containers.list(all=True):
            if service.lower() in c.name.lower():
                return c
    except Exception as e:
        logger.warning(f"Docker list failed: {e}")
    return None


def _do_restart(target: str) -> dict:
    """
    Hard-restart the container.
    Records timestamp for the circuit breaker's sliding window.
    """
    container = _find_container(target)
    if not container:
        return {"status": "error", "message": f"Container '{target}' not found"}
    try:
        container.restart(timeout=15)
        _record_restart(target)
        return {"status": "success", "container": container.name}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _do_scale_up(target: str) -> dict:
    """
    Docker Compose doesn't support replica scaling natively.
    We restart the container (clears transient state / memory leaks)
    and log the scale-up intent.
    In Swarm / Kubernetes this would call the orchestrator scale API.
    """
    container = _find_container(target)
    if not container:
        return {"status": "error", "message": f"Container '{target}' not found"}
    try:
        container.restart(timeout=10)
        return {
            "status":    "success",
            "container": container.name,
            "note":      "Docker Compose: restarted. Swarm/K8s would add replicas.",
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _do_rate_limit(target: str) -> dict:
    """
    Conceptual action — production would push a config change to nginx/Envoy
    or flip a Redis feature flag that the services check.
    Here we record intent clearly.
    """
    logger.warning(f"RATE LIMIT intent: '{target}' — update API gateway config")
    return {
        "status": "logged",
        "target": target,
        "note":   "Production: update API gateway rate-limit config",
    }


def _do_circuit_breaker(target: str) -> dict:
    """
    Trip the healing-agent's internal circuit breaker for this service.
    Future restart requests for the service are suppressed until the breaker resets.
    Production: push config to Envoy/Istio or a Redis feature flag.
    """
    _cb_open[target]     = True
    _cb_opened_at[target] = datetime.utcnow()
    logger.warning(
        f"CIRCUIT BREAKER tripped for '{target}' "
        f"— auto-resets in {_CB_RESET_MIN} min"
    )
    return {
        "status":             "tripped",
        "service":            target,
        "resets_in_minutes":  _CB_RESET_MIN,
        "note":               "Production: update Envoy/Istio circuit-breaker config",
    }


# ── Post-healing verification (background thread) ─────────────────────────
def _verify_async(record: dict, anomaly: dict) -> None:
    """
    Spawn a daemon thread that:
      1. Sleeps 120 seconds (give the system time to stabilise)
      2. Calls detect-now on the anomaly detector
      3. Updates the Q-table with the outcome
      4. Publishes the final record to healing.actions Kafka topic

    WHY a thread and not asyncio?
      actions.py is synchronous (called from both async and sync contexts).
      A daemon thread is the simplest approach that doesn't block callers
      and dies cleanly when the process exits.
    """
    started_epoch = record["started_at"]

    def _run() -> None:
        time.sleep(120)
        success = False
        try:
            resp = requests.post(
                f"{ANOMALY_DETECTOR_URL}/api/v1/anomalies/detect-now",
                timeout=15,
            )
            success = not resp.json().get("anomaly_detected", True)
        except Exception as e:
            logger.warning(f"Verification call failed: {e}")

        recovery_secs = round(time.time() - started_epoch, 1)
        record.update({
            "success":               success,
            "recovery_time_seconds": recovery_secs,
            "verified_at":           datetime.utcnow().isoformat(),
        })

        reward = 1.0 if success else -1.0
        _q_update(record["state"], record["action"], reward)
        _publish_healing_action(record, anomaly)

        logger.info(
            f"HEALING VERIFIED | action={record['action']} "
            f"target={record['target']} success={success} "
            f"recovery={recovery_secs:.0f}s reward={reward:+.1f}"
        )

    threading.Thread(target=_run, daemon=True).start()


# ── Main entry point ───────────────────────────────────────────────────────
def decide_and_heal(anomaly: dict) -> dict:
    """
    Full healing pipeline for one anomaly event.

    Returns the healing record immediately (success field is still None —
    it is populated asynchronously by the background verifier thread).
    """
    state  = _classify_state(anomaly)
    target = _pick_target(state, anomaly)
    action = _select_action(state)

    # Circuit-breaker guard: downgrade restart to alert if breaker is open
    if action == "restart_container" and _is_circuit_open(target):
        logger.warning(
            f"Circuit breaker OPEN for '{target}' — "
            f"downgrading restart → alert"
        )
        action = "alert"

    q_before = _Q.get(state, {}).get(action, 0.5)

    record: dict = {
        "action_id":      f"heal_{uuid.uuid4().hex[:8]}",
        "action":         action,
        "target":         target,
        "state":          state,
        "q_value_before": round(q_before, 4),
        "anomaly_score":  anomaly.get("anomaly_score"),
        "severity":       anomaly.get("severity"),
        "anomaly_id":     anomaly.get("anomaly_id"),
        "started_at":     time.time(),
        "healed_at":      datetime.utcnow().isoformat(),
        "success":        None,    # set by background verifier
    }

    # Execute the chosen action
    if action == "restart_container":
        exec_result = _do_restart(target)
    elif action == "scale_up":
        exec_result = _do_scale_up(target)
    elif action == "rate_limit":
        exec_result = _do_rate_limit(target)
    elif action == "circuit_breaker":
        exec_result = _do_circuit_breaker(target)
    else:   # alert
        exec_result = {
            "status":      "alerted",
            "description": anomaly.get("description", "Anomaly detected"),
        }
        logger.critical(
            f"HEALING ALERT | severity={anomaly.get('severity')} | "
            f"{anomaly.get('description')}"
        )

    record["execution_result"] = exec_result
    healing_history.insert(0, record)   # most recent first

    logger.info(
        f"HEALING | action={action} target={target} state={state} "
        f"Q={q_before:.3f} | exec_status={exec_result.get('status', '?')}"
    )

    # Launch background verifier
    _verify_async(record, anomaly)

    return record


# ── Public accessors ───────────────────────────────────────────────────────
def get_q_table() -> dict:
    """Return a deep copy of the current Q-table."""
    return {state: dict(actions) for state, actions in _Q.items()}


def get_circuit_breaker_status() -> dict:
    """Return circuit-breaker state for all services seen so far."""
    services = set(list(_cb_open.keys()) + list(_restart_ts.keys()))
    out = {}
    for svc in services:
        cutoff = datetime.utcnow() - timedelta(minutes=_CB_WINDOW_MIN)
        recent = [t for t in _restart_ts.get(svc, []) if t > cutoff]
        out[svc] = {
            "open":               bool(_cb_open.get(svc, False)),
            "restarts_in_window": len(recent),
            "window_minutes":     _CB_WINDOW_MIN,
            "max_before_open":    _CB_MAX_RESTARTS,
            "resets_in_minutes":  _CB_RESET_MIN,
        }
    return out


def compute_stats() -> dict:
    """Aggregate stats over all completed healing records."""
    completed = [r for r in healing_history if r.get("success") is not None]
    if not completed:
        return {"total": len(healing_history), "completed": 0}

    successes = [r for r in completed if r["success"]]
    rtimes    = [r["recovery_time_seconds"] for r in successes if r.get("recovery_time_seconds")]

    action_counts: dict = defaultdict(int)
    for r in healing_history:
        action_counts[r["action"]] += 1

    return {
        "total":             len(healing_history),
        "completed":         len(completed),
        "success_rate":      round(len(successes) / len(completed), 3) if completed else 0.0,
        "mttr_seconds":      round(sum(rtimes) / len(rtimes), 1) if rtimes else None,
        "action_counts":     dict(action_counts),
    }
