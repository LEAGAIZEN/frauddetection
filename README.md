# Real-Time Fraud Detection Platform

An end-to-end machine-learning and MLOps project for streaming credit-card transactions, scoring fraud risk, storing results, and exposing the results through an API and live dashboard.

> **Project status — October 6, 2026:** The repository contains a working foundation for the ingestion, streaming, model-training, API, database, container, Kubernetes, Terraform, and Ansible layers. The core backend, producer, Spark streaming job, database schema, and training workflow are implemented. Some surrounding production features are still incomplete or require additional configuration, including frontend screens, model-registry automation, database initialization code, Ansible roles, CI/CD, and monitoring manifests.

## Contents

- [What this project does](#what-this-project-does)
- [Architecture](#architecture)
- [End-to-end data flow](#end-to-end-data-flow)
- [Repository structure](#repository-structure)
- [Components](#components)
- [Machine-learning workflow](#machine-learning-workflow)
- [API](#api)
- [Database](#database)
- [Configuration](#configuration)
- [Running locally](#running-locally)
- [Running the individual services](#running-the-individual-services)
- [Docker Compose](#docker-compose)
- [Kubernetes and infrastructure](#kubernetes-and-infrastructure)
- [Current implementation status](#current-implementation-status)
- [Known limitations and next steps](#known-limitations-and-next-steps)
- [Security and production notes](#security-and-production-notes)

## What this project does

The platform is designed to process a stream of transactions and classify each transaction as normal or potentially fraudulent.

The main workflow is:

1. Read transactions from a credit-card fraud dataset.
2. Replay the transactions into Apache Kafka.
3. Consume the Kafka topic with Apache Spark Structured Streaming.
4. Store transaction data in PostgreSQL.
5. Train a Random Forest classifier from the stored data.
6. Track the experiment and register the model with MLflow.
7. Load the latest registered model in a FastAPI service.
8. Score transactions through the `/predict` endpoint.
9. Store the prediction and risk score in PostgreSQL.
10. Broadcast newly scored transactions over WebSocket to the frontend.
11. Expose Prometheus metrics for operational monitoring.

The repository is intended both as a fraud-detection demonstration and as an MLOps capstone showing how data ingestion, streaming, training, serving, storage, deployment, and monitoring fit together.

## Architecture

```text
                         +----------------------+
                         | Credit-card dataset  |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | producer/producer.py |
                         | Kafka producer       |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Apache Kafka          |
                         | topic: transactions  |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | streaming/spark.py   |
                         | Structured Streaming |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | PostgreSQL            |
                         | transactions table   |
                         +----------+-----------+
                                    |
                    +---------------+----------------+
                    |                                |
                    v                                v
          +-------------------+             +-------------------+
          | training/train.py |             | backend/main.py   |
          | Random Forest     |             | FastAPI            |
          +---------+---------+             +---------+---------+
                    |                                 |
                    v                                 v
          +-------------------+             +-------------------+
          | MLflow            |             | /predict           |
          | tracking/registry|             | /transactions      |
          +-------------------+             | /metrics           |
                                            | /ws                |
                                            +---------+---------+
                                                      |
                                                      v
                                            +-------------------+
                                            | React dashboard    |
                                            +-------------------+
```

## End-to-end data flow

### 1. Transaction production

`producer/producer.py` loads a CSV file with pandas and publishes each row as JSON to Kafka. The producer preserves the relative timing represented by the dataset's `Time` column, scaled by a configurable speed multiplier.

The default Kafka topic is `transactions`, and the default local broker is `localhost:9092`.

### 2. Kafka transport

Kafka provides the durable event stream between the producer and downstream consumers. The local Docker Compose configuration runs Kafka in single-node KRaft mode, so ZooKeeper is not required.

### 3. Spark stream processing

`streaming/spark.py` reads JSON messages from Kafka and parses them with a fixed schema containing:

- `Time`
- `V1` through `V28`
- `Amount`
- `Class`

Spark currently performs two streaming operations:

- It computes 10-second window statistics and prints transaction count, average amount, and fraud count to the console.
- It writes parsed transactions to PostgreSQL using `foreachBatch` and a JDBC connection.

The PostgreSQL stream uses a checkpoint directory at `streaming/checkpoints/postgres` so Spark can recover its progress.

### 4. Database storage

PostgreSQL stores the incoming transaction features in `transactions`. Predictions made by the API are stored separately in `predictions`, linked using `transaction_id`.

### 5. Model training

`training/train.py` reads the stored data from PostgreSQL, uses the first 80% of rows for training and the remaining 20% for testing, and trains a scikit-learn `RandomForestClassifier`.

The model uses:

- Features: `v1` through `v28` and `amount`
- Target: `class`
- `n_estimators=100`
- `max_depth=12`
- `class_weight=balanced_subsample`
- `random_state=42`
- `n_jobs=-1`

The script logs parameters, PR AUC, precision, recall, and the trained model to MLflow.

### 6. Model serving

At startup, `backend/main.py` loads a model in one of two ways:

- If `MODEL_PATH` is set, it loads a local MLflow model path.
- Otherwise, it searches the MLflow Model Registry for the highest model version under `MODEL_NAME` and loads that version.

The loaded model and SQLAlchemy database engine are stored in the process-wide `state` dictionary.

### 7. Prediction and live updates

`backend/predict.py` receives a validated transaction, constructs the feature vector in the same order used during training, calculates a fraud probability, and marks the transaction as flagged when the risk score is at least `0.5`.

It then:

1. Inserts the transaction into PostgreSQL.
2. Inserts its prediction into PostgreSQL.
3. Updates Prometheus counters and latency metrics.
4. Broadcasts the result to connected WebSocket clients.
5. Returns the transaction ID, amount, risk score, flag, and model version.

## Repository structure

```text
.
├── ansible/                         # Kafka host configuration files
│   ├── inventory.ini
│   ├── kafka-retention.conf.j2
│   ├── playbook.yml
│   ├── playbook.AAAAAA              # Empty placeholder file
│   └── roles/
├── backend/                         # FastAPI model-serving service
│   ├── Dockerfile
│   ├── export_model.py
│   ├── history.py
│   ├── live_feed.py
│   ├── main.py
│   ├── metrics.py
│   ├── mlflow.db
│   ├── predict.py
│   ├── requirements.txt
│   ├── schemas.py
│   └── state.py
├── database/                        # PostgreSQL schema and initialization area
│   ├── init_db.py
│   └── schema.sql
├── documents/                       # Planning documents and architecture images
│   ├── fraud-detection-capstone-plan.md
│   └── producerimages.png
├── frontend/                        # React/Vite dashboard
│   ├── Dockerfile
│   ├── index.html
│   ├── package.json
│   ├── package-lock.json
│   ├── src/
│   └── vite.config.js
├── k8s/                             # Kubernetes deployment manifests
│   ├── Configmap.yaml
│   ├── backend-deployment.yaml
│   ├── database-statefulset.yaml
│   └── frontend-deployment.yaml
├── producer/                        # Kafka transaction producer
│   ├── producer.py
│   └── requirements.txt
├── streaming/                       # Spark Structured Streaming job
│   ├── checkpoints/
│   ├── spark.py
│   └── sparkprocess.png
├── terraform/                       # Infrastructure provisioning
│   ├── main.tf
│   └── variables.tf
├── training/                        # Model training and MLflow artifacts
│   ├── mlflow_registry.py
│   ├── mlruns/
│   ├── requirements.txt
│   └── train.py
├── docker-compose.yaml              # Local Kafka and PostgreSQL services
├── inventory.ini                    # Root-level infrastructure inventory
├── mlflow.db                         # MLflow database file
├── playbook.yml                      # Root-level Ansible playbook
└── realationbetweensparkandkafka.png # Spark/Kafka relationship diagram
```

> `frontend/node_modules` is currently present in the repository listing. It should normally be excluded from version control and installed with `npm ci` instead.

## Components

### Producer: `producer/`

The producer converts each CSV row into JSON and sends it to Kafka.

Important command-line options:

- `--csv`: path to the input CSV file
- `--broker`: Kafka bootstrap server
- `--speed`: replay speed multiplier
- `--limit`: send only the first N rows for a quick test

Example:

```bash
python producer/producer.py --csv ../data/creditcard.csv --speed 200 --limit 1000
```

### Stream processor: `streaming/`

The Spark job is responsible for Kafka consumption, JSON parsing, window aggregation, and PostgreSQL persistence.

The current window aggregation is for visibility and operational analysis. The current prediction API does not use those window aggregates as model features; it uses the transaction's 28 numeric features and amount directly.

### Training: `training/`

The training service reads from PostgreSQL and logs the experiment to MLflow. Training is currently implemented as a time-ordered 80/20 split rather than a randomized split.

`mlflow_registry.py` exists as the intended location for model promotion or registry-management logic, but it is currently empty.

### Backend: `backend/`

The backend is a FastAPI application with four route modules:

| Module | Responsibility | Endpoint |
|---|---|---|
| `predict.py` | Score and persist a transaction | `POST /predict` |
| `history.py` | Return recent transactions and predictions | `GET /transactions` |
| `metrics.py` | Expose Prometheus metrics | `GET /metrics` |
| `live_feed.py` | Push scored transactions to clients | `WebSocket /ws` |

The application also provides:

- `GET /health` — returns service health and the loaded model version.
- Automatic request validation through the Pydantic `Transaction` schema.
- MLflow model loading during application startup.
- SQLAlchemy connections for PostgreSQL access.

### Frontend: `frontend/`

The frontend is a React application built with Vite. Its package configuration currently provides development, production build, and preview commands.

The intended dashboard consumes the API and WebSocket feed to display recent transactions, fraud flags, risk scores, and live updates. The frontend source directory exists, but the repository should be checked before deployment to confirm that all dashboard components and API connection settings are implemented.

### Database: `database/`

`database/schema.sql` creates two tables:

#### `transactions`

Stores the original transaction data, model features, class label when available, Kafka timestamp, and ingestion timestamp.

#### `predictions`

Stores the prediction linked to a transaction:

- `risk_score`
- `flagged`
- `model_version`
- `created_at`

Indexes are created for Kafka timestamps and transaction-to-prediction lookups.

### Docker Compose

`docker-compose.yaml` currently defines:

- Kafka on port `9092`
- PostgreSQL on host port `55432`, mapped to container port `5432`
- A persistent `pgdata` volume
- Automatic execution of `database/schema.sql` when the PostgreSQL data directory is initialized

MLflow, Spark, the backend, and frontend are not currently defined as Compose services in this file; they must be started separately unless additional service definitions are added.

### Kubernetes: `k8s/`

The Kubernetes directory contains manifests for:

- Backend deployment
- Frontend deployment
- PostgreSQL StatefulSet
- Shared configuration through a ConfigMap

Before applying these manifests, verify image names, environment variables, service definitions, storage configuration, and secrets for the target cluster.

### Terraform: `terraform/`

Terraform contains the infrastructure provisioning layer. `main.tf` and `variables.tf` are intended to define external infrastructure such as an object-storage bucket for raw data or model artifacts.

Run `terraform plan` before applying changes, and keep cloud credentials outside the repository.

### Ansible: `ansible/`

Ansible is intended to configure the Kafka host. The repository currently includes an inventory, a playbook, a Kafka retention template, and a roles directory. The role implementation should be completed and tested against the target host before relying on it for provisioning.

## Machine-learning workflow

The model uses the well-known credit-card fraud feature layout:

```text
Time, V1, V2, ..., V28, Amount, Class
```

The producer preserves `Class` in Kafka and Spark writes it to PostgreSQL. The training script uses `class` as the target. The API input schema intentionally does not require `class`, because production prediction requests should not need the true label.

The classification threshold is currently fixed at `0.5`:

```text
risk_score >= 0.5  -> flagged = true
risk_score <  0.5  -> flagged = false
```

For fraud detection, threshold selection should eventually be based on business costs, precision-recall analysis, and the desired false-positive rate rather than using a universal default.

## API

Assuming the backend is running at `http://localhost:8000`:

### Health check

```bash
curl http://localhost:8000/health
```

Example response:

```json
{
  "status": "ok",
  "model_version": "1"
}
```

### Score a transaction

`POST /predict` accepts `time_seconds`, `v1` through `v28`, and `amount`.

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "time_seconds": 123.45,
    "v1": 0.1,
    "v2": 0.2,
    "v3": 0.0,
    "v4": 0.0,
    "v5": 0.0,
    "v6": 0.0,
    "v7": 0.0,
    "v8": 0.0,
    "v9": 0.0,
    "v10": 0.0,
    "v11": 0.0,
    "v12": 0.0,
    "v13": 0.0,
    "v14": 0.0,
    "v15": 0.0,
    "v16": 0.0,
    "v17": 0.0,
    "v18": 0.0,
    "v19": 0.0,
    "v20": 0.0,
    "v21": 0.0,
    "v22": 0.0,
    "v23": 0.0,
    "v24": 0.0,
    "v25": 0.0,
    "v26": 0.0,
    "v27": 0.0,
    "v28": 0.0,
    "amount": 42.50
  }'
```

Example response shape:

```json
{
  "transaction_id": 1,
  "amount": 42.5,
  "risk_score": 0.013,
  "flagged": false,
  "model_version": 1
}
```

### Recent transactions

```bash
curl "http://localhost:8000/transactions?limit=50"
```

The maximum supported limit is 500.

### Prometheus metrics

```bash
curl http://localhost:8000/metrics
```

Current metrics include:

- `predictions_total`
- `fraud_flagged_total`
- `prediction_latency_seconds`

### WebSocket live feed

Connect a WebSocket client to:

```text
ws://localhost:8000/ws
```

Every successful prediction is broadcast to connected clients as JSON.

## Configuration

Create a `.env` file in the repository root and add the environment variables required by Kafka, PostgreSQL, and MLflow. Use values appropriate for your local or deployment environment. Keep all credentials private and do not commit them to the repository.

Example variable names only:

```dotenv
KAFKA_BROKER=...
KAFKA_TOPIC=...
POSTGRES_HOST=...
POSTGRES_PORT=...
POSTGRES_DB=...
POSTGRES_USER=...
POSTGRES_PASSWORD=...
MLFLOW_TRACKING_URI=...
MODEL_NAME=...
```

Do not commit real passwords, cloud credentials, API keys, or production connection strings. Add `.env` to `.gitignore`.

## Running locally

### Prerequisites

Install or make available:

- Python 3.10 or newer
- Docker and Docker Compose
- Node.js and npm
- Apache Kafka client dependencies
- Apache Spark 3.5.x with access to the Kafka and PostgreSQL connectors
- PostgreSQL client tools, if you want to inspect the database manually
- MLflow

### 1. Start Kafka and PostgreSQL

```bash
docker compose up -d kafka postgres
```

Check the containers:

```bash
docker compose ps
```

PostgreSQL is available from the host at port `55432`.

### 2. Install producer dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r producer/requirements.txt
```

On Windows PowerShell, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

### 3. Prepare the dataset

Place the input dataset at a path such as:

```text
data/creditcard.csv
```

The expected columns are `Time`, `V1` through `V28`, `Amount`, and `Class`.

### 4. Start the Spark stream

Run the Spark job from a Spark-enabled environment:

```bash
spark-submit streaming/spark.py
```

The job consumes Kafka messages, prints 10-second aggregates, and persists transactions to PostgreSQL.

### 5. Train and register the model

Install the training dependencies from `training/requirements.txt`, ensure PostgreSQL and MLflow settings are available, then run:

```bash
python training/train.py
```

The script creates or uses the `fraud-detection` MLflow experiment and registers the model as `fraud-model`.

### 6. Start the backend

Install backend dependencies:

```bash
pip install -r backend/requirements.txt
```

Start FastAPI from the `backend` directory so its local imports resolve correctly:

```bash
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Open the interactive API documentation at:

```text
http://localhost:8000/docs
```

### 7. Start the frontend

```bash
cd frontend
npm ci
npm run dev
```

The Vite development server will print the local URL. Configure the frontend API and WebSocket URLs according to the backend address.

## Running the individual services

### Producer only

```bash
python producer/producer.py \
  --csv data/creditcard.csv \
  --broker localhost:9092 \
  --speed 200 \
  --limit 1000
```

### Backend only

The backend requires:

- A reachable PostgreSQL database
- A reachable MLflow tracking store or a valid local model path
- A registered `fraud-model`, unless `MODEL_PATH` is provided

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Frontend only

```bash
cd frontend
npm ci
npm run dev
```

### Frontend production build

```bash
cd frontend
npm run build
npm run preview
```

## Kubernetes and infrastructure

The repository includes deployment-related files, but the manifests should be treated as environment-specific templates until image names, secrets, storage classes, service names, and connection settings are verified.

Typical workflow:

```bash
kubectl apply -f k8s/Configmap.yaml
kubectl apply -f k8s/database-statefulset.yaml
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/frontend-deployment.yaml
```

Before applying to a cluster:

1. Build and publish the backend and frontend images.
2. Update the image references in the manifests.
3. Create Kubernetes Secrets for passwords and credentials.
4. Configure persistent storage for PostgreSQL.
5. Confirm that backend-to-database and frontend-to-backend service names resolve.
6. Add readiness and liveness probes if they are not already provided by the target manifest version.

For Terraform:

```bash
cd terraform
terraform init
terraform validate
terraform plan
```

Do not run `terraform apply` until the provider, account, region, state backend, and variables have been reviewed.



## Known limitations and next steps

1. Add a reproducible root-level dependency strategy and pin compatible versions.
2. Add automated tests for producer serialization, API validation, database operations, and model prediction flow.
3. Add a proper database initialization/migration workflow.
4. Complete MLflow model promotion and champion/challenger handling.
5. Add model validation gates before registering a model.
6. Replace the in-memory WebSocket connection manager with a scalable broadcast mechanism if multiple backend replicas are deployed.
7. Add authentication and authorization to the API and dashboard.
8. Move all secrets to environment management, Docker secrets, or Kubernetes Secrets.
9. Complete and verify the React dashboard screens and API configuration.
10. Add Docker Compose services for MLflow, Spark, backend, frontend, and monitoring where practical.
11. Add CI/CD workflows for tests, image builds, vulnerability scanning, and deployment.
12. Add Prometheus scraping configuration and Grafana dashboards.
13. Choose and document a production fraud threshold based on business requirements.
14. Evaluate the model with time-aware validation and fraud-specific metrics such as PR AUC, recall at a fixed false-positive rate, and cost-weighted loss.
15. Add data drift and model-performance monitoring.
16. Remove committed generated artifacts and dependency directories such as `frontend/node_modules` when they are not required.

## Security and production notes

This project is an educational and demonstration platform in its current form. Before production use:

- Do not use the sample PostgreSQL credentials from `docker-compose.yaml`.
- Do not expose Kafka, PostgreSQL, MLflow, or the backend publicly without authentication and network controls.
- Use TLS for API, WebSocket, Kafka, PostgreSQL, and MLflow connections where appropriate.
- Validate and sanitize all input data.
- Restrict access to model artifacts and experiment data.
- Add audit logging for predictions and model changes.
- Protect the `/metrics` endpoint if it contains sensitive operational information.
- Use a managed secret store and rotate credentials.
- Pin container image versions instead of using floating tags such as `latest`.
- Back up PostgreSQL and MLflow metadata.
- Test rollback behavior for model and application deployments.

## License

No license file is currently present in the repository. Add a `LICENSE` file before distributing or accepting external contributions under defined terms.
