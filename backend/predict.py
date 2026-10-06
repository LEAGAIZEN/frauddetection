from fastapi import APIRouter
from sqlalchemy import text

from state import state
from schemas import Transaction
from metrics import PREDICTIONS_TOTAL, FRAUD_FLAGGED_TOTAL, PREDICTION_LATENCY
from live_feed import manager

import time

router = APIRouter()

FEATURE_ORDER = [f"v{i}" for i in range(1, 29)] + ["amount"]


@router.post("/predict")
async def predict(txn: Transaction):
    start = time.time()

    model = state["model"]
    features = [[getattr(txn, name) for name in FEATURE_ORDER]]
    risk_score = float(model.predict_proba(features)[0][1])
    flagged = risk_score >= 0.5

    with state["engine"].begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO transactions
                (time_seconds, v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,
                 v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,amount)
                VALUES
                (:time_seconds, :v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10,:v11,:v12,:v13,:v14,
                 :v15,:v16,:v17,:v18,:v19,:v20,:v21,:v22,:v23,:v24,:v25,:v26,:v27,:v28,:amount)
                RETURNING id
                """
            ),
            txn.model_dump(),
        )
        transaction_id = result.scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO predictions (transaction_id, risk_score, flagged, model_version)
                VALUES (:transaction_id, :risk_score, :flagged, :model_version)
                """
            ),
            {
                "transaction_id": transaction_id,
                "risk_score": risk_score,
                "flagged": flagged,
                "model_version": str(state["model_version"]),
            },
        )

    PREDICTIONS_TOTAL.inc()
    if flagged:
        FRAUD_FLAGGED_TOTAL.inc()
    PREDICTION_LATENCY.observe(time.time() - start)

    payload = {
        "transaction_id": transaction_id,
        "amount": txn.amount,
        "risk_score": risk_score,
        "flagged": flagged,
        "model_version": state["model_version"],
    }
    await manager.broadcast(payload)

    return payload