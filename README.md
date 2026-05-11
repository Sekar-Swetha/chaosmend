# ChaosMend

Self-healing distributed payments platform. The system breaks itself with controlled chaos experiments, detects breakage with ML (Isolation Forest + Prophet), and recovers automatically with a Q-learning agent.

## Architecture

Three layers on top of Kafka, Postgres, and Prometheus/Grafana.

**Application layer** — 4 FastAPI microservices
- `transaction` (8001) — accepts transactions, persists to Postgres, publishes `transaction.created` to Kafka
- `risk` (8002) — consumes `transaction.created`, flags high-risk, publishes `transaction.flagged`
- `notification` (8003) — consumes `transaction.flagged`, logs alerts
- `analytics` (8004) — exposes aggregated metrics from Postgres

**Intelligence layer**
- `chaos-agent` (8005) — injects failures (container kill, CPU stress, memory stress, network latency). Manual via REST or auto-scheduler.
- `anomaly-detector` (8006) — Isolation Forest (multivariate) + Prophet (time-series) over Prometheus + analytics metrics. Publishes `anomaly.detected`.
- `healing-agent` (8007) — consumes `anomaly.detected` (Kafka primary, REST poll fallback). Q-learning picks recovery action, executes via Docker socket, verifies after 120s, updates Q-table. Circuit breaker prevents restart loops.

**Infrastructure + Monitoring**
- Postgres 15, Kafka (Confluent 7.5) + Zookeeper, Kafka UI (8080)
- Prometheus (9090), Grafana (3000), cAdvisor (8090), kafka-exporter (9308)

## Event topics

| Topic | Producer | Consumers |
|-------|----------|-----------|
| `transaction.created` | transaction | risk, analytics |
| `transaction.flagged` | risk | notification |
| `chaos.events` | chaos-agent | (ML correlates with metrics) |
| `anomaly.detected` | anomaly-detector | healing-agent |
| `healing.actions` | healing-agent | (audit) |

## Prerequisites

- Docker + Docker Compose
- ~4 GB free RAM
- Ports 3000, 5432, 8001-8007, 8080, 8090, 9090, 9092, 9308, 29092 free

## Setup

```bash
git clone https://github.com/Sekar-Swetha/chaosmend.git
cd chaosmend
cp .env.example .env
# edit .env if you want non-default passwords
docker-compose up -d --build
```

Wait ~60s for healthchecks to pass. Verify:

```bash
docker-compose ps
curl http://localhost:8001/health
```

## Endpoints

| Service | URL | Purpose |
|---------|-----|---------|
| Transaction API | http://localhost:8001/docs | Create/list transactions |
| Risk API | http://localhost:8002/docs | Risk service status |
| Notification | http://localhost:8003/docs | Alert log |
| Analytics | http://localhost:8004/docs | Aggregated metrics |
| Chaos Agent | http://localhost:8005/docs | Trigger/configure chaos |
| Anomaly Detector | http://localhost:8006/docs | Anomaly list, model status, predictions |
| Healing Agent | http://localhost:8007/docs | Healing history, Q-table, circuit breakers |
| Kafka UI | http://localhost:8080 | Topics, consumer groups |
| Prometheus | http://localhost:9090 | Metrics, query UI |
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |
| cAdvisor | http://localhost:8090 | Container resource stats |

## Configuration

All config via environment variables. See `.env.example` for the full list. Key knobs:

- `RISK_THRESHOLD` — amount above which a transaction is flagged high-risk (default `1000`)
- `POLL_INTERVAL_SECONDS` — anomaly detector + healing agent polling cadence (default `30`)
- Anomaly sensitivity — `PUT /api/v1/anomalies/config` at runtime (`contamination`, `prophet_ci_multiplier`)
- Auto-chaos — `PUT /api/v1/chaos/config` at runtime (`enabled`, `interval_seconds`, `probability`)

## Repo layout

```
chaos_agent/        Chaos injection service
healing_agent/      Q-learning self-healing service
ml/                 Anomaly detection service (Isolation Forest + Prophet)
services/
  transaction/      Entry-point payments API
  risk/             Risk scoring consumer
  notification/     Alert consumer
  analytics/        Postgres aggregator
monitoring/
  prometheus.yml    Prometheus scrape config
  grafana/          Provisioned datasources + dashboards
docker-compose.yml  Full stack wiring
```

## Stopping

```bash
docker-compose down           # keep volumes
docker-compose down -v        # nuke Postgres/Prometheus/Grafana data
```

## License

Academic / educational use.
