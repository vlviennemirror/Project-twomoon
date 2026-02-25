import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from shared_lib import database
from shared_lib import redis_ipc

logger = logging.getLogger("twomoon.api.leaderboard")

router = APIRouter(prefix="/api/leaderboard", tags=["Leaderboard"])

VALID_FACTIONS = {"M", "F"}

LEADERBOARD_CACHE_TTL = 60
LEADERBOARD_CACHE_KEY_PREFIX = "cache:leaderboard"


@router.get("/{faction}")
async def get_leaderboard(
    faction: str,
    limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    faction = faction.upper()
    if faction not in VALID_FACTIONS:
        raise HTTPException(status_code=400, detail="Faction must be 'M' or 'F'")

    cache_key = f"{LEADERBOARD_CACHE_KEY_PREFIX}:{faction}:{limit}"

    try:
        cached = await redis_ipc.cache_get_json(cache_key)
        if cached is not None:
            logger.debug("Leaderboard cache HIT: %s", cache_key)
            return cached
    except Exception as e:
        logger.warning("Leaderboard cache read failed (%s), querying DB directly", e)

    count_row = await database.fetchval(
        "SELECT COUNT(*) FROM user_levels WHERE faction = $1",
        faction,
    )
    total_members = int(count_row or 0)

    rows = await database.fetch(
        "SELECT "
        "  ul.user_id, "
        "  ul.xp, "
        "  ul.level, "
        "  ul.total_messages, "
        "  ROW_NUMBER() OVER (ORDER BY ul.xp DESC, ul.level DESC) AS rank "
        "FROM user_levels ul "
        "WHERE ul.faction = $1 "
        "ORDER BY ul.xp DESC, ul.level DESC "
        "LIMIT $2",
        faction,
        limit,
    )

    if not rows:
        result = {
            "faction": faction,
            "entries": [],
            "total_members": total_members,
        }
        await _write_cache(cache_key, result)
        return result

    user_ids = [str(r["user_id"]) for r in rows]

    profile_rows = await database.fetch(
        "SELECT user_id, display_name "
        "FROM user_profiles "
        "WHERE user_id = ANY($1::text[])",
        user_ids,
    )
    name_map: dict[str, str] = {
        str(r["user_id"]): r["display_name"]
        for r in profile_rows
        if r.get("display_name")
    }

    entries = []
    for r in rows:
        uid = str(r["user_id"])
        entries.append({
            "rank": int(r["rank"]),
            "user_id": uid,
            "username": name_map.get(uid, f"User-{uid[:6]}"),
            "level": int(r["level"]),
            "xp": int(r["xp"]),
            "total_messages": int(r["total_messages"] or 0),
        })

    result = {
        "faction": faction,
        "entries": entries,
        "total_members": total_members,
    }

    await _write_cache(cache_key, result)
    return result


async def _write_cache(key: str, value: dict) -> None:
    """Write to Redis cache, swallowing errors to keep the API non-fatal on Redis failure."""
    try:
        await redis_ipc.cache_set_json(key, value, ttl_seconds=LEADERBOARD_CACHE_TTL)
        logger.debug("Leaderboard cache SET: %s (TTL=%ds)", key, LEADERBOARD_CACHE_TTL)
    except Exception as e:
        logger.warning("Leaderboard cache write failed: %s", e)