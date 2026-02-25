import logging
from typing import Any

from fastapi import APIRouter, Depends, Query

from shared_lib import database
from web_hub.api.auth import require_clearance

logger = logging.getLogger("twomoon.api.moderation")

router = APIRouter(prefix="/api/moderation", tags=["Moderation"])


@router.get("/metrics")
async def get_moderation_metrics(
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    row = await database.fetchrow(
        "SELECT "
        "  COUNT(*) AS total_caught, "
        "  COUNT(*) FILTER (WHERE source != 'AI') AS total_regex, "
        "  COUNT(*) FILTER (WHERE source = 'AI') AS total_ai, "
        "  COALESCE(AVG(confidence) FILTER (WHERE source = 'AI' AND confidence IS NOT NULL), 0) "
        "    AS avg_confidence, "
        "  COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE) AS strikes_today "
        "FROM moderation_strikes"
    )

    return {
        "total_caught": row["total_caught"] or 0,
        "total_regex": row["total_regex"] or 0,
        "total_ai": row["total_ai"] or 0,
        "avg_confidence": round(float(row["avg_confidence"] or 0), 4),
        "circuit_breaker_status": await _get_circuit_breaker_status(),
        "strikes_today": row["strikes_today"] or 0,
    }


@router.get("/strikes")
async def get_strikes(
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=15, ge=1, le=100),
) -> dict:
    offset = (page - 1) * page_size

    total = await database.fetchval(
        "SELECT COUNT(*) FROM moderation_strikes"
    )

    rows = await database.fetch(
        "SELECT "
        "  ms.id, "
        "  ms.user_id, "
        "  ms.tier, "
        "  ms.reason, "
        "  ms.confidence, "
        "  ms.source, "
        "  ms.created_at "
        "FROM moderation_strikes ms "
        "ORDER BY ms.created_at DESC "
        "LIMIT $1 OFFSET $2",
        page_size,
        offset,
    )

    user_ids = list({str(r["user_id"]) for r in rows})

    name_map: dict[str, str] = {}
    if user_ids:
        profile_rows = await database.fetch(
            "SELECT user_id, display_name "
            "FROM user_profiles "
            "WHERE user_id = ANY($1::text[])",
            user_ids,
        )
        name_map = {
            str(r["user_id"]): r["display_name"]
            for r in profile_rows if r.get("display_name")
        }

    strikes = []
    for r in rows:
        uid = str(r["user_id"])
        strikes.append({
            "id": str(r["id"]),
            "user_id": uid,
            "username": name_map.get(uid, f"User-{uid[:6]}"),
            "tier": r["tier"],
            "reason": r["reason"] or "",
            "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
            "source": r["source"] or "REGEX",
            "created_at": r["created_at"].isoformat() if r["created_at"] else "",
        })

    return {
        "strikes": strikes,
        "total": total or 0,
        "page": page,
        "page_size": page_size,
    }


@router.get("/strikes/{strike_id}")
async def get_strike_detail(
    strike_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    row = await database.fetchrow(
        "SELECT "
        "  ms.id, ms.guild_id, ms.user_id, ms.moderator_id, "
        "  ms.tier, ms.reason, ms.confidence, ms.source, "
        "  ms.message_content, ms.action_taken, ms.created_at "
        "FROM moderation_strikes ms "
        "WHERE ms.id = $1",
        strike_id,
    )

    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Strike not found")

    uid = str(row["user_id"])
    profile = await database.fetchrow(
        "SELECT display_name FROM user_profiles WHERE user_id = $1",
        uid,
    )
    username = profile["display_name"] if profile and profile.get("display_name") else f"User-{uid[:6]}"

    return {
        "id": str(row["id"]),
        "guild_id": str(row["guild_id"]) if row["guild_id"] else None,
        "user_id": uid,
        "username": username,
        "moderator_id": str(row["moderator_id"]) if row["moderator_id"] else "Apostle",
        "tier": row["tier"],
        "reason": row["reason"] or "",
        "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
        "source": row["source"] or "REGEX",
        "message_content": row["message_content"] if user.get("clearance") in ("owner", "admin") else "[REDACTED]",
        "action_taken": row["action_taken"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else "",
    }


async def _get_circuit_breaker_status() -> str:
    from shared_lib import redis_ipc
    try:
        status = await redis_ipc.cache_get("apostle:circuit_breaker:status")
        if status:
            return status
    except Exception:
        pass
    return "UNKNOWN"
