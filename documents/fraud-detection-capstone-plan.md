# Real-Time Fraud Detection — Capstone Project Plan

**Goal:** End-to-end MLOps showcase project integrating every tool from Phases 1–10 (Bash/Git, ML fundamentals, Docker, AWS, MLflow, SQL/Airflow, Kubernetes, CI/CD, Prometheus/Grafana, Terraform, Ansible, Spark, Kafka) into one working system, for the job-search portfolio.

**Status:** Planning stage — dataset decision (real Kaggle vs. synthetic) still open before Step 1 begins.

---

## 1. Repo Structure

```
mlops-practice/capstone-fraud/
│
├── data/
│   └── transactions.csv              # raw/seed dataset
│
├── producer/
│   ├── producer.py                   # reads data, streams to Kafka 'transactions' topic
│   └── requirements.txt
│
├── streaming/
│   └── spark_job.py                  # consumes Kafka, feature engineering, writes to DB + Parquet
│
├── training/
│   ├── train.py                      # trains model, logs to MLflow
│   ├── mlflow_registry.py            # promote champion/challenger
│   └── requirements.txt
│
├── database/
│   ├── schema.sql                    # transactions, predictions tables
│   ├── migrations/
│   └── init_db.py
│
├── backend/                          # FastAPI
│   ├── main.py
│   ├── routes/
│   │   ├── predict.py                # POST /predict
│   │   ├── metrics.py                # GET /metrics (Prometheus)
│   │   └── history.py                # GET /transactions (dashboard stats)
│   ├── websocket/
│   │   └── live_feed.py              # WS /ws - pushes live scored transactions
│   ├── models/
│   │   └── db_models.py              # SQLAlchemy models
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/                         # React
│   ├── src/
│   │   ├── components/
│   │   │   ├── LiveFeed.jsx
│   │   │   ├── StatsPanel.jsx
│   │   │   └── AlertBadge.jsx
│   │   ├── hooks/
│   │   │   └── useWebSocket.js
│   │   └── App.jsx
│   ├── Dockerfile
│   └── package.json
│
├── k8s/
│   ├── backend-deployment.yaml
│   ├── frontend-deployment.yaml
│   ├── database-statefulset.yaml     # Postgres needs persistent storage
│   ├── database-pvc.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── servicemonitor.yaml
│   └── prometheusrule.yaml
│
├── terraform/
│   └── main.tf                       # S3 bucket for model artifacts/raw data
│
├── ansible/
│   ├── inventory.ini
│   ├── playbook.yml
│   └── roles/
│       └── kafka-broker/
│
├── .github/workflows/
│   └── ci-cd.yml
│
└── README.md
```

---

## 2. Tech Stack Per Component

| Layer | Tool | What it does here |
|---|---|---|
| Ingestion | **Kafka** (KRaft mode) | `producer.py` streams transactions into a `transactions` topic |
| Stream processing | **Spark** (structured streaming) | Consumes Kafka, computes rolling/window features, writes to Postgres + Parquet |
| Storage | **PostgreSQL** | Persists `transactions` and `predictions` tables |
| Experimentation | **MLflow** | Tracks training runs, hosts model registry, champion/challenger aliases |
| Serving | **FastAPI (backend)** | `/predict`, `/metrics` (Prometheus), `/ws` (live feed), `/transactions` (history) |
| UI | **React (frontend)** | Live transaction feed, fraud-risk highlighting, rolling stats panel |
| Containerization | **Docker** | Images for backend, frontend, and (optionally) Spark job |
| Orchestration | **Kubernetes** | Deployments (backend, frontend), StatefulSet (Postgres), ConfigMaps/Secrets, probes |
| Infra provisioning | **Terraform** | Provisions S3 bucket for artifacts/raw data |
| Config management | **Ansible** | Configures the Kafka broker host — apt module, handlers, roles |
| CI/CD | **GitHub Actions** | Test → build (backend + frontend images) → tag rewrite → deploy |
| Monitoring | **Prometheus + Grafana** | Scrapes `/metrics`, dashboards, `PrometheusRule` alert on fraud-rate spikes |
| Drift detection (stretch) | **Evidently** | Detects feature/data drift on the incoming transaction stream |

---

## 3. Database Schema (PostgreSQL)

**`transactions`**
- `id`, `account_id`, `amount`, `merchant`, `location`, `timestamp`, engineered features (from Spark)

**`predictions`**
- `id`, `transaction_id` (FK), `risk_score`, `flagged` (bool), `model_version`, `timestamp`

---

## 4. Data Flow (End to End)

```
Kafka (producer.py)
   -> Spark (feature engineering)
      -> PostgreSQL (transactions table)
         -> training/train.py (reads features) -> MLflow (tracking + registry)
            -> backend/predict.py (loads champion model)
               -> writes to predictions table
               -> pushes to WebSocket (/ws)
                  -> frontend LiveFeed.jsx (real-time display)
               -> backend/metrics.py exposes Prometheus metrics
                  -> Grafana dashboards + PrometheusRule alerts
```

---

## 5. Build Order

1. **Data** — pick real Kaggle dataset or synthetic generator (blocking decision)
2. **Kafka** — producer streaming into `transactions` topic
3. **Spark** — consume + engineer features, write to Postgres
4. **Database** — schema, migrations, init script
5. **Training + MLflow** — train classifier, log runs, promote champion
6. **Backend (FastAPI)** — `/predict`, `/metrics`, `/ws`, `/transactions`, Dockerfile
7. **Frontend (React)** — live feed, stats panel, Dockerfile
8. **Kubernetes** — Deployments (backend, frontend), StatefulSet + PVC (Postgres), ConfigMaps/Secrets, probes
9. **Terraform + Ansible** — S3 bucket provisioning; Kafka broker config via apt/handlers/roles
10. **CI/CD (GitHub Actions)** — test → build → tag rewrite → deploy
11. **Monitoring** — ServiceMonitor, PrometheusRule, Grafana dashboards
12. **Stretch: Evidently drift detection** on the live transaction stream

Each stage should run and be verified standalone (not just trusted from tool output) before the next stage depends on it.

---

## 6. Prerequisites Checklist (verify before Step 1)

| Tool | Check |
|---|---|
| Docker Desktop | `docker ps` clean |
| Kafka | container up on 9092, KRaft mode |
| PySpark | `spark.py` runs on Windows |
| MLflow | tracking server reachable |
| kind cluster | `kubectl config current-context` = `kind-<name>` |
| Terraform + LocalStack | image pinned to `3.4.0`, `s3_use_path_style = true` |
| Ansible (WSL2) | `dark1@AIZEN` reachable, `inventory.ini` present |
| GitHub Actions runner | self-hosted runner registered |
| Prometheus/Grafana | `kube-prometheus-stack` Helm release installed |

---

## 7. Persistent Weak Spots to Re-Test During This Build

- **R²** — if any regression-style evaluation shows up, re-test this cold before trusting it.
- **kubectl/terraform context assumptions** — always verify current state before assuming a prior step worked.
- New for this project: **StatefulSets + PersistentVolumeClaims** (not covered in Phase 7 — first time touching stateful workloads in k8s).

---

## 8. Open Decisions

- [ ] Real Kaggle "Credit Card Fraud Detection" dataset vs. synthetic transaction generator
- [ ] Whether to embed Grafana panels in the React frontend or keep them separate

---

## 9. After This Capstone

Job search prep: public repo with polished README, tailored resume/LinkedIn, mock technical interviews, start applying before feeling "fully ready."
