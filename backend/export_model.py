import os
import shutil

import mlflow
import mlflow.sklearn
from dotenv import load_dotenv

ENV_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(ENV_PATH)

def require(name):
    v = os.environ.get(name)
    if not v:
        raise SystemExit(
            f"Missing required env var '{name}'. Expected it in {ENV_PATH}."
        )
    return v

MLFLOW_TRACKING_URI = require("MLFLOW_TRACKING_URI")
MLFLOW_ARTIFACT_ROOT = require("MLFLOW_ARTIFACT_ROOT")
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud-model")

os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
os.environ["MLFLOW_ARTIFACT_ROOT"] = MLFLOW_ARTIFACT_ROOT

EXPORT_PATH = os.path.join(os.path.dirname(__file__), "model_export")

print(f"Tracking URI:  {MLFLOW_TRACKING_URI}")
print(f"Artifact root: {MLFLOW_ARTIFACT_ROOT}")
print(f"Model name:    {MODEL_NAME}")
print(f"Export path:   {EXPORT_PATH}")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
client = mlflow.MlflowClient()

versions = [v for v in client.search_model_versions(f"name='{MODEL_NAME}'") if v.source]
if not versions:
    raise SystemExit(f"No usable versions of {MODEL_NAME}")

latest = max(versions, key=lambda v: int(v.version))
print(f"Selected version: {latest.version}")
print(f"Source:           {latest.source}")

model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{latest.version}")

if os.path.exists(EXPORT_PATH):
    shutil.rmtree(EXPORT_PATH)

mlflow.sklearn.save_model(model, path=EXPORT_PATH)
print(f"Exported {MODEL_NAME} version {latest.version} to {EXPORT_PATH}")