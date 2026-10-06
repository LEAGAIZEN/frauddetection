import os
from contextlib import asynccontextmanager

import mlflow
import mlflow.sklearn
from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy import create_engine

from state import state

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ["POSTGRES_PORT"]
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

MLFLOW_TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud-model")


@asynccontextmanager
async def lifespan(app: FastAPI):
    model_path = os.environ.get("MODEL_PATH")
    if model_path:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        state["model"] = mlflow.sklearn.load_model(model_path)
        state["model_version"] = os.environ.get("MODEL_VERSION", "unknown")
        print(f"Loaded model from local path {model_path} (version tag: {state['model_version']})")
    else:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = mlflow.MlflowClient()
        latest = max(
            client.search_model_versions(f"name='{MODEL_NAME}'"),
            key=lambda v: int(v.version),
        )
        state["model"] = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{latest.version}")
        state["model_version"] = latest.version
        print(f"Loaded {MODEL_NAME} version {latest.version} from MLflow registry")

    state["engine"] = create_engine(DB_URL)
    yield
    state.clear()


app = FastAPI(title="Fraud Detection API", lifespan=lifespan)

import predict, metrics, history, live_feed  # noqa: E402

app.include_router(predict.router)
app.include_router(metrics.router)
app.include_router(history.router)
app.include_router(live_feed.router)


@app.get("/health")
def health():
    return {"status": "ok", "model_version": state.get("model_version")}