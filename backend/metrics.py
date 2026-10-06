from fastapi import APIRouter, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

PREDICTIONS_TOTAL = Counter("predictions_total", "Total number of transactions scored")
FRAUD_FLAGGED_TOTAL = Counter("fraud_flagged_total", "Total number of transactions flagged as fraud")
PREDICTION_LATENCY = Histogram("prediction_latency_seconds", "Time spent scoring one transaction")

router = APIRouter()


@router.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)