import os
import mlflow
import mlflow.sklearn
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, precision_score, recall_score

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ["POSTGRES_PORT"]
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]
EXPERIMENT = "fraud-detection"
MODEL_NAME = "fraud-model"

FEATURES = [f"v{i}" for i in range(1, 29)] + ["amount"]
TARGET = "class"

PARAMS = {
    "n_estimators": 100,
    "max_depth": 12,
    "class_weight": "balanced_subsample",
    "random_state": 42,
    "n_jobs": -1,
}


def load_data():
    engine = create_engine(DB_URL)
    query = "SELECT time_seconds, " + ", ".join(FEATURES) + ", class FROM transactions ORDER BY time_seconds"
    return pd.read_sql(query, engine)


def main():
    df = load_data()
    split = int(len(df) * 0.8)
    train, test = df.iloc[:split], df.iloc[split:]

    X_train, y_train = train[FEATURES], train[TARGET]
    X_test, y_test = test[FEATURES], test[TARGET]

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run():
        model = RandomForestClassifier(**PARAMS)
        model.fit(X_train, y_train)

        scores = model.predict_proba(X_test)[:, 1]
        preds = (scores >= 0.5).astype(int)

        mlflow.log_params(PARAMS)
        mlflow.log_param("train_rows", len(train))
        mlflow.log_param("test_rows", len(test))
        mlflow.log_param("test_fraud", int(y_test.sum()))
        mlflow.log_metric("pr_auc", average_precision_score(y_test, scores))
        mlflow.log_metric("precision", precision_score(y_test, preds, zero_division=0))
        mlflow.log_metric("recall", recall_score(y_test, preds))

        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(3),
        )

        print("train rows:", len(train), "| test rows:", len(test), "| test fraud:", int(y_test.sum()))
        print("pr_auc:", round(average_precision_score(y_test, scores), 4))
        print("precision:", round(precision_score(y_test, preds, zero_division=0), 4))
        print("recall:", round(recall_score(y_test, preds), 4))


if __name__ == "__main__":
    main()