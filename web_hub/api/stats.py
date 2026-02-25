import logging
from typing import Any

from fastapi import APIRouter, Depends

from shared_lib import database
from web_hub.api.auth import require_clearance

logger = logging.getLogger("twomoon.api.stats")

router = APIRouter(prefix="/api/stats", tags=["Statistics"])


@router.get("/overview")
async def get_overview(
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    """
    Aggregated dashboard metrics. Runs 4 targeted COUNT/SUM queries — each
    hits an indexed column so RU cost is minimal on CockroachDB Serverless.
    """

    level_row = await database.fetchrow(
        "SELECT COUNT(DISTINCT user_id) AS total_users, "
        "       COALESCE(SUM(xp), 0)   AS total_xp "
        "FROM user_levels"
    )

    vouched_row = await database.fetchrow(
        "SELECT COUNT(*) AS total_vouched "
        "FROM vouch_codes WHERE status = 'USED'"
    )

    strike_row = await database.fetchrow(
        "SELECT COUNT(*)                                                   AS total_strikes, "
        "       COALESCE(AVG(confidence) FILTER (WHERE source = 'AI'), 0) AS avg_ai_confidence "
        "FROM moderation_strikes"
    )

    active_row = await database.fetchrow(
        "SELECT COUNT(DISTINCT user_id) AS active_today "
        "FROM user_factions "
        "WHERE last_active_date = CURRENT_DATE"
    )

    return {
        "total_users":        int(level_row["total_users"] or 0),
        "total_xp_processed": int(level_row["total_xp"] or 0),
        "total_vouched":      int(vouched_row["total_vouched"] or 0),
        "total_strikes":      int(strike_row["total_strikes"] or 0),
        "avg_ai_confidence":  round(float(strike_row["avg_ai_confidence"] or 0.0), 4),
        "active_today":       int(active_row["active_today"] or 0),
    }