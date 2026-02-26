import logging
import time
from typing import Any, Optional

from shared_lib import database
from shared_lib import redis_ipc

logger = logging.getLogger("twomoon.fleet_publisher")

FLEET_COMMAND_CHANNEL = "twomoon:fleet_commands"
FLEET_STATUS_CHANNEL = "events:fleet_status"
AGENT_HEARTBEAT_KEY = "fleet:agent:heartbeat"
BOT_STATUS_KEY_PREFIX = "fleet:status:"
FLEET_REGISTRY_KEY = "fleet:registry"


async def publish_fleet_command(
    action: str,
    bot_id: str,
    requested_by: Optional[str] = None,
    extra: Optional[dict] = None,
) -> int:
    import json
    r = await redis_ipc.get_redis()
    payload = json.dumps({"command": action.lower(), "bot_id": bot_id})
    subscriber_count = await r.publish(FLEET_COMMAND_CHANNEL, payload)
    logger.info(
        "Fleet command published: action=%s bot_id=%s subscribers=%d",
        action,
        bot_id[:8],
        subscriber_count,
    )
    return subscriber_count


async def start_bot(bot_id: str, requested_by: Optional[str] = None) -> dict:
    row = await database.fetchrow(
        "SELECT bot_id, is_active, bot_type FROM bots WHERE bot_id = $1",
        bot_id,
    )

    if not row:
        raise ValueError(f"Bot {bot_id[:8]} not found in fleet registry")

    if not row["is_active"]:
        await database.execute(
            "UPDATE bots SET is_active = TRUE, updated_at = now() WHERE bot_id = $1",
            bot_id,
        )
        logger.info("Bot %s activated in database", bot_id[:8])

    subscribers = await publish_fleet_command("START", bot_id, requested_by)

    return {
        "status": "command_published",
        "action": "START",
        "bot_id": bot_id,
        "agent_reachable": subscribers > 0,
    }


async def stop_bot(bot_id: str, requested_by: Optional[str] = None) -> dict:
    row = await database.fetchrow(
        "SELECT bot_id FROM bots WHERE bot_id = $1",
        bot_id,
    )

    if not row:
        raise ValueError(f"Bot {bot_id[:8]} not found in fleet registry")

    await database.execute(
        "UPDATE bots SET is_active = FALSE, updated_at = now() WHERE bot_id = $1",
        bot_id,
    )

    subscribers = await publish_fleet_command("STOP", bot_id, requested_by)

    return {
        "status": "command_published",
        "action": "STOP",
        "bot_id": bot_id,
        "agent_reachable": subscribers > 0,
    }


async def restart_bot(bot_id: str, requested_by: Optional[str] = None) -> dict:
    row = await database.fetchrow(
        "SELECT bot_id, is_active FROM bots WHERE bot_id = $1",
        bot_id,
    )

    if not row:
        raise ValueError(f"Bot {bot_id[:8]} not found in fleet registry")

    if not row["is_active"]:
        await database.execute(
            "UPDATE bots SET is_active = TRUE, updated_at = now() WHERE bot_id = $1",
            bot_id,
        )

    subscribers = await publish_fleet_command("RESTART", bot_id, requested_by)

    return {
        "status": "command_published",
        "action": "RESTART",
        "bot_id": bot_id,
        "agent_reachable": subscribers > 0,
    }


async def request_status(bot_id: str) -> dict:
    await publish_fleet_command("STATUS", bot_id)
    cached = await get_bot_status(bot_id)
    return cached if cached else {"bot_id": bot_id, "status": "UNKNOWN"}


async def get_bot_status(bot_id: str) -> Optional[dict]:
    return await redis_ipc.cache_get_json(f"{BOT_STATUS_KEY_PREFIX}{bot_id}")


async def get_fleet_status() -> list[dict]:
    registry_raw = await redis_ipc.cache_get_json(FLEET_REGISTRY_KEY)
    if not registry_raw or not isinstance(registry_raw, list):
        return []

    statuses = []
    for bot_id in registry_raw:
        status = await get_bot_status(bot_id)
        if status:
            statuses.append(status)
        else:
            statuses.append({"bot_id": bot_id, "status": "UNKNOWN"})

    return statuses


async def get_agent_health() -> dict:
    raw = await redis_ipc.cache_get(AGENT_HEARTBEAT_KEY)
    if not raw:
        return {"agent_online": False, "last_seen": None}

    try:
        last_ts = float(raw)
        age = time.time() - last_ts
        return {
            "agent_online": age < 90.0,
            "last_seen_seconds_ago": round(age, 1),
            "last_seen_timestamp": last_ts,
        }
    except (ValueError, TypeError):
        return {"agent_online": False, "last_seen": None}


async def subscribe_fleet_status(callback) -> None:
    await redis_ipc.subscribe_to_channel(FLEET_STATUS_CHANNEL, callback)
