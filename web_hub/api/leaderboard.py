import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from shared_lib import database

logger = logging.getLogger("twomoon.api.leaderboard")

router = APIRouter(prefix="/api/leaderboard", tags=["Leaderboard"])

VALID_FACTIONS = {"M", "F"}


@router.get("/{faction}")
async def get_leaderboard(
    faction: str,
    limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    faction = faction.upper()
    if faction not in VALID_FACTIONS:
        raise HTTPException(status_code=400, detail="Faction must be 'M' or 'F'")

    count_row = await database.fetchval(
        "SELECT COUNT(*) FROM user_levels WHERE faction = $1",
        faction,
    )
    total_members = count_row or 0

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
        return {
            "faction": faction,
            "entries": [],
            "total_members": total_members,
        }

    user_ids = [str(r["user_id"]) for r in rows]

    profile_rows = await database.fetch(
        "SELECT user_id, display_name "
        "FROM user_profiles "
        "WHERE user_id = ANY($1::text[])",
        user_ids,
    )
    name_map: dict[str, str] = {
        str(r["user_id"]): r["display_name"] for r in profile_rows if r.get("display_name")
    }

    entries = []
    for r in rows:
        uid = str(r["user_id"])
        entries.append({
            "rank": r["rank"],
            "user_id": uid,
            "username": name_map.get(uid, f"User-{uid[:6]}"),
            "level": r["level"],
            "xp": r["xp"],
            "total_messages": r["total_messages"] or 0,
        })

    return {
        "faction": faction,
        "entries": entries,
        "total_members": total_members,
    }
