import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, APIRouter, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared_lib import database
from shared_lib import redis_ipc
from web_hub.services import fleet_publisher
from web_hub.api.auth import router as auth_router, require_clearance

from web_hub.api.config import router as api_config_router
from web_hub.api.leaderboard import router as leaderboard_router
from web_hub.api.moderation import router as moderation_router
from web_hub.api.stats import router as stats_router

LOG_FORMAT = "[%(asctime)s] [%(name)-24s] [%(levelname)-7s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    stream=sys.stdout,
)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

logger = logging.getLogger("twomoon.web_hub")

CORS_ORIGINS = [
    o.strip()
    for o in os.environ.get("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",")
    if o.strip()
]

SPA_DIST_DIR = Path(__file__).resolve().parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=== Two Moon Web Hub — Startup Sequence Initiated ===")

    logger.info("[1/2] Connecting to CockroachDB...")
    try:
        await database.get_pool()
        logger.info("[1/2] CockroachDB pool ready")
    except Exception as e:
        logger.critical("FATAL: CockroachDB connection failed: %s", e)
        raise

    logger.info("[2/2] Connecting to Redis...")
    try:
        await redis_ipc.get_redis()
        logger.info("[2/2] Redis connection ready")
    except Exception as e:
        logger.critical("FATAL: Redis connection failed: %s", e)
        await database.close_pool()
        raise

    agent_health = await fleet_publisher.get_agent_health()
    if agent_health.get("agent_online"):
        logger.info("AlmaLinux agent detected (online)")
    else:
        logger.warning("AlmaLinux agent NOT detected — fleet commands will queue in DB")

    logger.info("=== Two Moon Web Hub — ONLINE ===")

    yield

    logger.info("=== Two Moon Web Hub — Shutdown Sequence Initiated ===")

    logger.info("[1/2] Closing Redis IPC...")
    try:
        await redis_ipc.shutdown()
        logger.info("[1/2] Redis IPC closed")
    except Exception as e:
        logger.error("Redis shutdown error: %s", e)

    logger.info("[2/2] Closing CockroachDB pool...")
    try:
        await database.close_pool()
        logger.info("[2/2] CockroachDB pool closed")
    except Exception as e:
        logger.error("Database shutdown error: %s", e)

    logger.info("=== Two Moon Web Hub — OFFLINE ===")


app = FastAPI(
    title="Two Moon — Web Hub",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if os.environ.get("TWOMOON_ENV") != "production" else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=600,
)

app.include_router(auth_router)
app.include_router(api_config_router)
app.include_router(leaderboard_router)
app.include_router(moderation_router)
app.include_router(stats_router)


fleet_router = APIRouter(prefix="/api/fleet", tags=["Fleet Management"])


@fleet_router.get("/status")
async def fleet_status(
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    statuses = await fleet_publisher.get_fleet_status()
    agent = await fleet_publisher.get_agent_health()
    return {
        "agent": agent,
        "fleet_size": len(statuses),
        "bots": statuses,
    }


@fleet_router.get("/{bot_id}/status")
async def bot_status(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin", "moderator"),
) -> dict:
    status = await fleet_publisher.get_bot_status(bot_id)
    if not status:
        status = await fleet_publisher.request_status(bot_id)
    return status


@fleet_router.post("/{bot_id}/start")
async def start_bot(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    try:
        result = await fleet_publisher.start_bot(bot_id, requested_by=user.get("sub"))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    await _record_audit(user, "FLEET_START", "bot", bot_id)
    return result


@fleet_router.post("/{bot_id}/stop")
async def stop_bot(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    try:
        result = await fleet_publisher.stop_bot(bot_id, requested_by=user.get("sub"))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    await _record_audit(user, "FLEET_STOP", "bot", bot_id)
    return result


@fleet_router.post("/{bot_id}/restart")
async def restart_bot(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    try:
        result = await fleet_publisher.restart_bot(bot_id, requested_by=user.get("sub"))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    await _record_audit(user, "FLEET_RESTART", "bot", bot_id)
    return result


app.include_router(fleet_router)


config_router = APIRouter(prefix="/api/config", tags=["Configuration"])


@config_router.post("/{bot_id}/invalidate")
async def invalidate_config(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    subscriber_count = await redis_ipc.publish_event(
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


@config_router.post("/{bot_id}/rules/invalidate")
async def invalidate_rules(
    bot_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    subscriber_count = await redis_ipc.publish_event(
        redis_ipc.build_config_channel(bot_id),
        "RULES_INVALIDATION",
        {"triggered_by": user.get("sub")},
    )

    await _record_audit(user, "RULES_INVALIDATE", "moderation_rules", bot_id)

    return {
        "status": "published",
        "subscribers_notified": subscriber_count,
    }


@config_router.post("/guild/{guild_id}/settings/invalidate")
async def invalidate_guild_settings(
    guild_id: str,
    user: dict[str, Any] = require_clearance("owner", "admin"),
) -> dict:
    subscriber_count = await redis_ipc.publish_event(
        redis_ipc.build_guild_channel(guild_id, "settings"),
        "GUILD_SETTINGS_INVALIDATION",
        {"triggered_by": user.get("sub")},
    )

    await _record_audit(user, "GUILD_SETTINGS_INVALIDATE", "guild_settings", guild_id)

    return {
        "status": "published",
        "subscribers_notified": subscriber_count,
    }


app.include_router(config_router)


@app.get("/health")
async def health_check() -> dict:
    db_ok = False
    redis_ok = False

    try:
        await database.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass

    try:
        redis_ok = await redis_ipc.health_check()
    except Exception:
        pass

    agent = await fleet_publisher.get_agent_health()
    fleet = await fleet_publisher.get_fleet_status()
    alive_count = sum(1 for b in fleet if b.get("status") == "RUNNING")

    all_healthy = db_ok and redis_ok and agent.get("agent_online", False)
    status = "healthy" if all_healthy else "degraded"

    return {
        "status": status,
        "services": {
            "cockroachdb": "up" if db_ok else "down",
            "redis": "up" if redis_ok else "down",
            "agent": agent,
        },
        "fleet": {
            "total": len(fleet),
            "alive": alive_count,
        },
    }


async def _record_audit(
    user: dict[str, Any],
    action: str,
    target_type: str,
    target_id: str,
) -> None:
    details = json.dumps({
        "clearance": user.get("clearance", "unknown"),
        "username": user.get("username", "unknown"),
    })

    try:
        await database.execute(
            "INSERT INTO audit_log (actor_id, action, target_type, target_id, details) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            user.get("sub", "unknown"),
            action,
            target_type,
            target_id,
            details,
        )
    except Exception as e:
        logger.error("Audit log write failed: %s", e)


if SPA_DIST_DIR.is_dir():
    from fastapi.responses import FileResponse

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = SPA_DIST_DIR / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(SPA_DIST_DIR / "index.html")

    app.mount("/assets", StaticFiles(directory=SPA_DIST_DIR / "assets"), name="static-assets")
    logger.info("SPA static files mounted from %s", SPA_DIST_DIR)
