import json
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from shared_lib import database
from shared_lib import redis_ipc
from web_hub.api.auth import require_clearance

logger = logging.getLogger("twomoon.api.config")

router = APIRouter(prefix="/api/config", tags=["Configuration CRUD"])


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


@router.get("/bot/{bot_id}", response_model=BotConfigResponse)
async def get_bot_config(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> BotConfigResponse:
    row = await database.fetchrow(
        "SELECT bot_id, guild_id, ai_system_prompt, ai_model_id, "
        "log_channel_id, feature_flags, moderation_config "
        "FROM bot_configs WHERE bot_id = $1",
        bot_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail="Bot config not found")

    return BotConfigResponse(
        bot_id=str(row["bot_id"]),
        guild_id=str(row["guild_id"]) if row["guild_id"] else None,
        ai_system_prompt=row["ai_system_prompt"],
        ai_model_id=row["ai_model_id"],
        log_channel_id=row["log_channel_id"],
        feature_flags=row["feature_flags"] or {},
        moderation_config=row["moderation_config"] or {},
    )


@router.put("/bot/{bot_id}", response_model=BotConfigResponse)
async def update_bot_config(
    bot_id: str,
    body: BotConfigUpdate,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> BotConfigResponse:
    existing = await database.fetchrow(
        "SELECT bot_id, guild_id FROM bot_configs WHERE bot_id = $1",
        bot_id,
    )

    if not existing:
        raise HTTPException(status_code=404, detail="Bot config not found")

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clauses = []
    params = []
    param_idx = 1

    for field_name, value in updates.items():
        if field_name in ("feature_flags", "moderation_config"):
            set_clauses.append(f"{field_name} = ${param_idx}::jsonb")
            params.append(json.dumps(value))
        else:
            set_clauses.append(f"{field_name} = ${param_idx}")
            params.append(value)
        param_idx += 1

    set_clauses.append("updated_at = now()")
    params.append(bot_id)

    query = (
        f"UPDATE bot_configs SET {', '.join(set_clauses)} "
        f"WHERE bot_id = ${param_idx}"
    )

    await database.execute(query, *params)

    mutated_fields = list(updates.keys())

    subscriber_count = await redis_ipc.publish_event(
        redis_ipc.build_config_channel(bot_id),
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
    row = await database.fetchrow(
        "SELECT * FROM guild_settings WHERE guild_id = $1",
        guild_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail="Guild settings not found")

    return GuildSettingsResponse(
        guild_id=str(row["guild_id"]),
        level_base=row.get("level_base", 100),
        level_exponent=float(row.get("level_exponent", 1.5)),
        msg_xp_min=row.get("msg_xp_min", 15),
        msg_xp_max=row.get("msg_xp_max", 25),
        msg_cooldown_sec=row.get("msg_cooldown_sec", 60),
        react_cooldown_sec=row.get("react_cooldown_sec", 120),
        react_xp=row.get("react_xp", 5),
        voice_xp_per_min=row.get("voice_xp_per_min", 10),
        announce_enabled=row.get("announce_enabled", True),
        announce_channel_id=row.get("announce_channel_id"),
        feature_overrides=row.get("feature_overrides") or {},
    )


@router.put("/guild/{guild_id}", response_model=GuildSettingsResponse)
async def update_guild_settings(
    guild_id: str,
    body: GuildSettingsUpdate,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> GuildSettingsResponse:
    existing = await database.fetchrow(
        "SELECT guild_id FROM guild_settings WHERE guild_id = $1",
        guild_id,
    )

    if not existing:
        raise HTTPException(status_code=404, detail="Guild settings not found")

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_clauses = []
    params = []
    param_idx = 1

    for field_name, value in updates.items():
        if field_name == "feature_overrides":
            set_clauses.append(f"{field_name} = ${param_idx}::jsonb")
            params.append(json.dumps(value))
        else:
            set_clauses.append(f"{field_name} = ${param_idx}")
            params.append(value)
        param_idx += 1

    set_clauses.append("updated_at = now()")
    params.append(guild_id)

    query = (
        f"UPDATE guild_settings SET {', '.join(set_clauses)} "
        f"WHERE guild_id = ${param_idx}"
    )

    await database.execute(query, *params)

    subscriber_count = await redis_ipc.publish_event(
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
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    rows = await database.fetch(
        "SELECT b.bot_id, b.bot_type, b.guild_id, b.is_active, "
        "bc.ai_model_id, bc.feature_flags "
        "FROM bots b LEFT JOIN bot_configs bc ON b.bot_id = bc.bot_id "
        "ORDER BY b.created_at ASC"
    )

    bots = []
    for r in rows:
        bots.append({
            "bot_id": str(r["bot_id"]),
            "bot_type": r["bot_type"],
            "guild_id": str(r["guild_id"]) if r["guild_id"] else None,
            "is_active": r["is_active"],
            "ai_model_id": r.get("ai_model_id"),
            "feature_flags": r.get("feature_flags") or {},
        })

    return {"bots": bots}


async def _record_audit(
    user: dict[str, Any],
    action: str,
    target_type: str,
    target_id: str,
    extra: Optional[dict] = None,
) -> None:
    details = {
        "clearance": user.get("clearance", "unknown"),
        "username": user.get("username", "unknown"),
    }
    if extra:
        details.update(extra)

    try:
        await database.execute(
            "INSERT INTO audit_log (actor_id, action, target_type, target_id, details) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            user.get("sub", "unknown"),
            action,
            target_type,
            target_id,
            json.dumps(details),
        )
    except Exception as e:
        logger.error("Audit log write failed: %s", e)
