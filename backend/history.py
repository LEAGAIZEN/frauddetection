from fastapi import APIRouter, Query
from sqlalchemy import text

from state import state

router = APIRouter()


@router.get("/transactions")
def recent_transactions(limit: int = Query(50, le=500)):
    query = text(
        """
        SELECT t.id, t.amount, t.ingested_at, p.risk_score, p.flagged, p.model_version
        FROM transactions t
        LEFT JOIN predictions p ON p.transaction_id = t.id
        ORDER BY t.id DESC
        LIMIT :limit
        """
    )
    with state["engine"].connect() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]