import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shared_lib import database
from shared_lib import redis_ipc
from web_hub.api.auth import require_clearance
from web_hub.services.audit import record_audit

logger = logging.getLogger("twomoon.api.config")

router = APIRouter(prefix="/api/config", tags=["Configuration"])


class BotConfigResponse(BaseModel):
    bot_id: str
    guild_id: Optional[str] = None
    ai_system_prompt: Optional[str] = None
    ai_model_id: Optional[str] = None
    log_channel_id: Optional[str] = None
    feature_flags: dict[str, Any] = Field(default_factory=dict)
    moderation_config: dict[str, Any] = Field(default_factory=dict)


class BotConfigUpdate(BaseModel):
    ai_system_prompt: Optional[str] = None
    ai_model_id: Optional[str] = None
    log_channel_id: Optional[str] = None
    feature_flags: Optional[dict[str, Any]] = None
    moderation_config: Optional[dict[str, Any]] = None


class GuildSettingsResponse(BaseModel):
    guild_id: str
    level_base: int = 100
    level_exponent: float = 1.5
    msg_xp_min: int = 15
    msg_xp_max: int = 25
    msg_cooldown_sec: int = 60
    react_cooldown_sec: int = 120
    react_xp: int = 5
    voice_xp_per_min: int = 10
    announce_enabled: bool = True
    announce_channel_id: Optional[str] = None
    feature_overrides: dict[str, Any] = Field(default_factory=dict)


class GuildSettingsUpdate(BaseModel):
    level_base: Optional[int] = None
    level_exponent: Optional[float] = None
    msg_xp_min: Optional[int] = None
    msg_xp_max: Optional[int] = None
    msg_cooldown_sec: Optional[int] = None
    react_cooldown_sec: Optional[int] = None
    react_xp: Optional[int] = None
    voice_xp_per_min: Optional[int] = None
    announce_enabled: Optional[bool] = None
    announce_channel_id: Optional[str] = None
    feature_overrides: Optional[dict[str, Any]] = None


_ALLOWED_BOT_FIELDS = {
    "ai_system_prompt",
    "ai_model_id",
    "log_channel_id",
    "feature_flags",
    "moderation_config",
}

_ALLOWED_GUILD_FIELDS = {
    "level_base",
    "level_exponent",
    "msg_xp_min",
    "msg_xp_max",
    "msg_cooldown_sec",
    "react_cooldown_sec",
    "react_xp",
    "voice_xp_per_min",
    "announce_enabled",
    "announce_channel_id",
    "feature_overrides",
}

def _serialize_json_field(value: dict[str, Any]) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        safe = {k: str(v) for k, v in (value or {}).items()}
        return json.dumps(safe, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


async def _safe_publish(channel: str, event: str, payload: dict[str, Any]) -> int:
    try:
        return await redis_ipc.publish_event(channel, event, payload)
    except Exception as e:
        logger.warning("Redis publish failed for %s: %s", channel, e)
        return 0


_record_audit = record_audit


@router.get("/bot/{bot_id}", response_model=BotConfigResponse)
async def get_bot_config(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> BotConfigResponse:
    try:
        row = await database.fetchrow(
            "SELECT bot_id, guild_id, ai_system_prompt, ai_model_id, "
            "log_channel_id, feature_flags, moderation_config "
            "FROM bot_configs WHERE bot_id = $1",
            bot_id,
        )
    except Exception as e:
        logger.error("DB error fetching bot_config %s: %s", bot_id, e)
        raise HTTPException(status_code=500, detail="Database error")

    if not row:
        raise HTTPException(status_code=404, detail="Bot config not found")

    return BotConfigResponse(
        bot_id=str(row["bot_id"]),
        guild_id=str(row["guild_id"]) if row["guild_id"] else None,
        ai_system_prompt=row.get("ai_system_prompt"),
        ai_model_id=row.get("ai_model_id"),
        log_channel_id=row.get("log_channel_id"),
        feature_flags=row.get("feature_flags") or {},
        moderation_config=row.get("moderation_config") or {},
    )


@router.put("/bot/{bot_id}", response_model=BotConfigResponse)
async def update_bot_config(
    bot_id: str,
    body: BotConfigUpdate,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> BotConfigResponse:
    try:
        existing = await database.fetchrow(
            "SELECT bot_id, guild_id FROM bot_configs WHERE bot_id = $1",
            bot_id,
        )
    except Exception as e:
        logger.error("DB error checking existing bot_config %s: %s", bot_id, e)
        raise HTTPException(status_code=500, detail="Database error")

    if not existing:
        raise HTTPException(status_code=404, detail="Bot config not found")

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    invalid = [f for f in updates.keys() if f not in _ALLOWED_BOT_FIELDS]
    if invalid:
        raise HTTPException(
            status_code=400, detail=f"Invalid fields in update: {invalid}"
        )

    set_clauses = []
    params: list[Any] = []
    param_idx = 1

    for field_name, value in updates.items():
        if field_name in ("feature_flags", "moderation_config"):
            params.append(_serialize_json_field(value or {}))
            set_clauses.append(f"{field_name} = ${param_idx}::jsonb")
        else:
            params.append(value)
            set_clauses.append(f"{field_name} = ${param_idx}")
        param_idx += 1

    set_clauses.append("updated_at = now()")
    params.append(bot_id)

    query = (
        f"UPDATE bot_configs SET {', '.join(set_clauses)} "
        f"WHERE bot_id = ${param_idx}"
    )

    try:
        await database.execute(query, *params)
    except Exception as e:
        logger.error("DB error updating bot_config %s: %s", bot_id, e)
        raise HTTPException(status_code=500, detail="Failed to update bot config")

    mutated_fields = list(updates.keys())

    channel = redis_ipc.build_config_channel(bot_id)
    subscriber_count = await _safe_publish(
        channel,
        "CONFIG_INVALIDATION",
        {
            "mutated_fields": mutated_fields,
            "triggered_by": user.get("sub"),
            "source": "config_api",
        },
    )

    logger.info(
        "Bot config updated: bot_id=%s fields=%s subscribers=%d by=%s",
        bot_id[:8],
        mutated_fields,
        subscriber_count,
        user.get("sub"),
    )

    await _record_audit(
        user,
        "CONFIG_UPDATE",
        "bot_config",
        bot_id,
        {"mutated_fields": mutated_fields, "subscribers_notified": subscriber_count},
    )

    return await get_bot_config(bot_id, user)


@router.get("/guild/{guild_id}", response_model=GuildSettingsResponse)
async def get_guild_settings(
    guild_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> GuildSettingsResponse:
    try:
        row = await database.fetchrow(
            "SELECT * FROM guild_settings WHERE guild_id = $1",
            guild_id,
        )
    except Exception as e:
        logger.error("DB error fetching guild_settings %s: %s", guild_id, e)
        raise HTTPException(status_code=500, detail="Database error")

    if not row:
        raise HTTPException(status_code=404, detail="Guild settings not found")

    return GuildSettingsResponse(
        guild_id=str(row["guild_id"]),
        level_base=int(row.get("level_base", 100)),
        level_exponent=float(row.get("level_exponent", 1.5)),
        msg_xp_min=int(row.get("msg_xp_min", 15)),
        msg_xp_max=int(row.get("msg_xp_max", 25)),
        msg_cooldown_sec=int(row.get("msg_cooldown_sec", 60)),
        react_cooldown_sec=int(row.get("react_cooldown_sec", 120)),
        react_xp=int(row.get("react_xp", 5)),
        voice_xp_per_min=int(row.get("voice_xp_per_min", 10)),
        announce_enabled=bool(row.get("announce_enabled", True)),
        announce_channel_id=row.get("announce_channel_id"),
        feature_overrides=row.get("feature_overrides") or {},
    )


@router.put("/guild/{guild_id}", response_model=GuildSettingsResponse)
async def update_guild_settings(
    guild_id: str,
    body: GuildSettingsUpdate,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> GuildSettingsResponse:
    try:
        existing = await database.fetchrow(
            "SELECT guild_id FROM guild_settings WHERE guild_id = $1",
            guild_id,
        )
    except Exception as e:
        logger.error("DB error checking guild_settings %s: %s", guild_id, e)
        raise HTTPException(status_code=500, detail="Database error")

    if not existing:
        raise HTTPException(status_code=404, detail="Guild settings not found")

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    invalid = [f for f in updates.keys() if f not in _ALLOWED_GUILD_FIELDS]
    if invalid:
        raise HTTPException(
            status_code=400, detail=f"Invalid fields in update: {invalid}"
        )

    set_clauses = []
    params: list[Any] = []
    param_idx = 1

    for field_name, value in updates.items():
        if field_name == "feature_overrides":
            params.append(_serialize_json_field(value or {}))
            set_clauses.append(f"{field_name} = ${param_idx}::jsonb")
        else:
            params.append(value)
            set_clauses.append(f"{field_name} = ${param_idx}")
        param_idx += 1

    set_clauses.append("updated_at = now()")
    params.append(guild_id)

    query = (
        f"UPDATE guild_settings SET {', '.join(set_clauses)} "
        f"WHERE guild_id = ${param_idx}"
    )

    try:
        await database.execute(query, *params)
    except Exception as e:
        logger.error("DB error updating guild_settings %s: %s", guild_id, e)
        raise HTTPException(status_code=500, detail="Failed to update guild settings")

    subscriber_count = await _safe_publish(
        redis_ipc.build_guild_channel(guild_id, "settings"),
        "GUILD_SETTINGS_INVALIDATION",
        {
            "mutated_fields": list(updates.keys()),
            "triggered_by": user.get("sub"),
            "source": "config_api",
        },
    )

    logger.info(
        "Guild settings updated: guild=%s fields=%s subscribers=%d",
        guild_id,
        list(updates.keys()),
        subscriber_count,
    )

    await _record_audit(
        user,
        "GUILD_SETTINGS_UPDATE",
        "guild_settings",
        guild_id,
        {"mutated_fields": list(updates.keys())},
    )

    return await get_guild_settings(guild_id, user)


@router.get("/bots")
async def list_configurable_bots(
    limit: int = Query(100, ge=1, le=1000),
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    try:
        rows = await database.fetch(
            "SELECT b.bot_id, b.bot_type, b.guild_id, b.is_active, "
            "bc.ai_model_id, bc.feature_flags "
            "FROM bots b LEFT JOIN bot_configs bc ON b.bot_id = bc.bot_id "
            "ORDER BY b.created_at ASC "
            "LIMIT $1",
            limit,
        )
    except Exception as e:
        logger.error("DB error listing bots: %s", e)
        raise HTTPException(status_code=500, detail="Database error")

    return {
        "bots": [
            {
                "bot_id": str(r["bot_id"]),
                "bot_type": r.get("bot_type"),
                "guild_id": str(r["guild_id"]) if r.get("guild_id") else None,
                "is_active": bool(r.get("is_active")),
                "ai_model_id": r.get("ai_model_id"),
                "feature_flags": r.get("feature_flags") or {},
            }
            for r in rows
        ]
    }


@router.post("/{bot_id}/invalidate")
async def invalidate_config(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    """Force the bot to hot-reload its full config from CockroachDB immediately."""
    subscriber_count = await _safe_publish(
        redis_ipc.build_config_channel(bot_id),
        "CONFIG_INVALIDATION",
        {"triggered_by": user.get("sub"), "scope": "full"},
    )

    await _record_audit(user, "CONFIG_INVALIDATE", "bot_config", bot_id)

    return {
        "status": "published",
        "channel": redis_ipc.build_config_channel(bot_id),
        "subscribers_notified": subscriber_count,
    }


@router.post("/{bot_id}/rules/invalidate")
async def invalidate_rules(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    """Force the bot to hot-reload moderation rules from CockroachDB immediately."""
    subscriber_count = await _safe_publish(
        redis_ipc.build_config_channel(bot_id),
        "RULES_INVALIDATION",
        {"triggered_by": user.get("sub")},
    )

    await _record_audit(user, "RULES_INVALIDATE", "moderation_rules", bot_id)

    return {
        "status": "published",
        "subscribers_notified": subscriber_count,
    }


@router.post("/guild/{guild_id}/settings/invalidate")
async def invalidate_guild_settings(
    guild_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    """Force all bots in this guild to hot-reload guild settings immediately."""
    subscriber_count = await _safe_publish(
        redis_ipc.build_guild_channel(guild_id, "settings"),
        "GUILD_SETTINGS_INVALIDATION",
        {"triggered_by": user.get("sub")},
    )

    await _record_audit(user, "GUILD_SETTINGS_INVALIDATE", "guild_settings", guild_id)

    return {
        "status": "published",
        "subscribers_notified": subscriber_count,
    }