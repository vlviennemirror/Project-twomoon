import asyncio
import ctypes
import ctypes.util
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from shared_lib import database
from shared_lib import redis_ipc

LOG_FORMAT = "[%(asctime)s] [%(name)-22s] [%(levelname)-7s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    stream=sys.stdout,
)

logger = logging.getLogger("twomoon.agent")

FLEET_COMMAND_CHANNEL = "events:fleet_command"
FLEET_STATUS_CHANNEL = "events:fleet_status"
AGENT_HEARTBEAT_KEY = "fleet:agent:heartbeat"
BOT_STATUS_KEY_PREFIX = "fleet:status:"
FLEET_REGISTRY_KEY = "fleet:registry"
BOT_STATUS_TTL = 120

CORE_NODE_SCRIPT = str(Path(__file__).resolve().parent / "core_node" / "main.py")
PYTHON_EXECUTABLE = os.environ.get("TWOMOON_PYTHON", sys.executable)
GRACEFUL_TIMEOUT = 10.0
HEARTBEAT_INTERVAL = 30.0


class BotStatus(str, Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    EXITED = "EXITED"
    CRASHED = "CRASHED"


@dataclass
class ProcessEntry:
    bot_id: str
    process: asyncio.subprocess.Process
    monitor_task: asyncio.Task
    pid: int
    status: BotStatus = BotStatus.STARTING
    spawned_at: float = field(default_factory=time.time)
    exit_code: Optional[int] = None


PROCESS_REGISTRY: dict[str, ProcessEntry] = {}
_registry_lock = asyncio.Lock()


def _make_preexec_fn():
    def _set_pdeathsig():
        try:
            libc_name = ctypes.util.find_library("c")
            if not libc_name:
                return
            libc = ctypes.CDLL(libc_name, use_errno=True)
            PR_SET_PDEATHSIG = 1
            result = libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0)
            if result != 0:
                errno = ctypes.get_errno()
                logger.warning("prctl(PR_SET_PDEATHSIG) returned errno=%d", errno)
        except (OSError, AttributeError):
            pass
    return _set_pdeathsig


async def _drain_stream(
    stream: asyncio.StreamReader,
    bot_id: str,
    stream_name: str,
) -> None:
    log_fn = logger.info if stream_name == "stdout" else logger.warning
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                log_fn("[%s] [%s] %s", bot_id[:8], stream_name, decoded)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Drain error %s/%s: %s", bot_id[:8], stream_name, e)


async def _publish_bot_status(bot_id: str, status: str, **extra: Any) -> None:
    status_data = {
        "bot_id": bot_id,
        "status": status,
        "timestamp": time.time(),
        **extra,
    }

    try:
        await redis_ipc.cache_set_json(
            f"{BOT_STATUS_KEY_PREFIX}{bot_id}",
            status_data,
            ttl_seconds=BOT_STATUS_TTL,
        )
    except Exception as e:
        logger.error("Failed to SET bot status for %s: %s", bot_id[:8], e)

    try:
        await redis_ipc.publish_event(
            FLEET_STATUS_CHANNEL,
            "BOT_STATUS_UPDATE",
            status_data,
        )
    except Exception as e:
        logger.error("Failed to PUBLISH bot status for %s: %s", bot_id[:8], e)


async def _sync_registry_to_redis() -> None:
    async with _registry_lock:
        bot_ids = list(PROCESS_REGISTRY.keys())
    try:
        await redis_ipc.cache_set_json(FLEET_REGISTRY_KEY, bot_ids, ttl_seconds=BOT_STATUS_TTL)
    except Exception:
        pass


async def _monitor_and_reap(bot_id: str, proc: asyncio.subprocess.Process) -> None:
    stdout_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None

    try:
        if proc.stdout:
            stdout_task = asyncio.create_task(
                _drain_stream(proc.stdout, bot_id, "stdout"),
                name=f"drain-stdout-{bot_id[:8]}",
            )
        if proc.stderr:
            stderr_task = asyncio.create_task(
                _drain_stream(proc.stderr, bot_id, "stderr"),
                name=f"drain-stderr-{bot_id[:8]}",
            )

        exit_code = await proc.wait()

        for task in (stdout_task, stderr_task):
            if task and not task.done():
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except asyncio.TimeoutError:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        final_status: str = BotStatus.EXITED.value

        async with _registry_lock:
            entry = PROCESS_REGISTRY.get(bot_id)
            if entry:
                entry.exit_code = exit_code
                if entry.status == BotStatus.STOPPING:
                    entry.status = BotStatus.EXITED
                elif exit_code == 0:
                    entry.status = BotStatus.EXITED
                else:
                    entry.status = BotStatus.CRASHED

                uptime = time.time() - entry.spawned_at
                final_status = entry.status.value

                logger.info(
                    "Bot reaped: %s pid=%d exit=%d status=%s uptime=%.0fs",
                    bot_id[:8], entry.pid, exit_code, entry.status.value, uptime,
                )
            else:
                logger.warning(
                    "Bot reaped but no registry entry found: %s pid=%d exit=%d — "
                    "publishing EXITED as safe fallback",
                    bot_id[:8], proc.pid, exit_code,
                )

            PROCESS_REGISTRY.pop(bot_id, None)

        await _publish_bot_status(
            bot_id,
            final_status,
            exit_code=exit_code,
            pid=proc.pid,
        )
        await _sync_registry_to_redis()

    except asyncio.CancelledError:
        for task in (stdout_task, stderr_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass


async def spawn_bot(bot_id: str) -> ProcessEntry:
    async with _registry_lock:
        existing = PROCESS_REGISTRY.get(bot_id)
        if existing and existing.process.returncode is None:
            raise RuntimeError(f"Bot {bot_id[:8]} already running (pid={existing.pid})")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    logger.info("Spawning bot: %s", bot_id[:8])

    proc = await asyncio.create_subprocess_exec(
        PYTHON_EXECUTABLE,
        CORE_NODE_SCRIPT,
        "--bot-id",
        bot_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=env,
        preexec_fn=_make_preexec_fn(),
    )

    entry = ProcessEntry(
        bot_id=bot_id,
        process=proc,
        monitor_task=asyncio.create_task(
            _monitor_and_reap(bot_id, proc),
            name=f"reaper-{bot_id[:8]}",
        ),
        pid=proc.pid,
        status=BotStatus.RUNNING,
    )

    async with _registry_lock:
        PROCESS_REGISTRY[bot_id] = entry

    logger.info("Bot spawned: %s pid=%d", bot_id[:8], proc.pid)

    await _publish_bot_status(bot_id, BotStatus.RUNNING.value, pid=proc.pid)
    await _sync_registry_to_redis()

    return entry


async def terminate_bot(bot_id: str) -> Optional[int]:
    async with _registry_lock:
        entry = PROCESS_REGISTRY.get(bot_id)
        if not entry:
            raise KeyError(f"Bot {bot_id[:8]} not in local process registry")
        if entry.process.returncode is not None:
            PROCESS_REGISTRY.pop(bot_id, None)
            return entry.process.returncode
        entry.status = BotStatus.STOPPING

    await _publish_bot_status(bot_id, BotStatus.STOPPING.value, pid=entry.pid)
    logger.info("Terminating bot: %s pid=%d (SIGTERM)", bot_id[:8], entry.pid)

    try:
        entry.process.terminate()
    except ProcessLookupError:
        async with _registry_lock:
            PROCESS_REGISTRY.pop(bot_id, None)
        return -1

    try:
        exit_code = await asyncio.wait_for(entry.process.wait(), timeout=GRACEFUL_TIMEOUT)
        logger.info("Bot terminated gracefully: %s exit=%d", bot_id[:8], exit_code)
        return exit_code
    except asyncio.TimeoutError:
        logger.warning("Bot %s did not exit in %.0fs, escalating to SIGKILL", bot_id[:8], GRACEFUL_TIMEOUT)

    try:
        entry.process.kill()
        exit_code = await asyncio.wait_for(entry.process.wait(), timeout=5.0)
        logger.info("Bot killed: %s exit=%d", bot_id[:8], exit_code)
        return exit_code
    except (asyncio.TimeoutError, ProcessLookupError) as e:
        logger.error("Kill failed for %s: %s", bot_id[:8], e)
        return None
    finally:
        async with _registry_lock:
            PROCESS_REGISTRY.pop(bot_id, None)
        await _sync_registry_to_redis()


async def restart_bot(bot_id: str) -> ProcessEntry:
    try:
        await terminate_bot(bot_id)
    except KeyError:
        pass
    await asyncio.sleep(1.0)
    return await spawn_bot(bot_id)


async def _handle_fleet_command(event_type: str, payload: dict) -> None:
    if event_type != "FLEET_COMMAND":
        return

    action = payload.get("action", "").upper()
    bot_id = payload.get("bot_id")
    requested_by = payload.get("requested_by", "unknown")

    if not bot_id:
        logger.warning("Fleet command missing bot_id: %s", payload)
        return

    logger.info(
        "Fleet command received: action=%s bot_id=%s from=%s",
        action, bot_id[:8], requested_by,
    )

    try:
        if action == "START":
            await spawn_bot(bot_id)

        elif action == "STOP":
            await terminate_bot(bot_id)

        elif action == "RESTART":
            await restart_bot(bot_id)

        elif action == "STATUS":
            entry = PROCESS_REGISTRY.get(bot_id)
            if entry:
                await _publish_bot_status(
                    bot_id,
                    entry.status.value,
                    pid=entry.pid,
                    uptime=round(time.time() - entry.spawned_at, 1),
                )
            else:
                await _publish_bot_status(bot_id, "NOT_RUNNING")

        else:
            logger.warning("Unknown fleet action: %s", action)

    except RuntimeError as e:
        logger.warning("Fleet command failed: %s", e)
        await _publish_bot_status(bot_id, "ERROR", error=str(e))
    except KeyError as e:
        logger.warning("Fleet command failed: %s", e)
        await _publish_bot_status(bot_id, "NOT_RUNNING", error=str(e))
    except Exception as e:
        logger.error("Fleet command error: %s", e, exc_info=True)
        await _publish_bot_status(bot_id, "ERROR", error=str(e))


async def _heartbeat_loop() -> None:
    logger.info("Heartbeat loop started (interval=%.0fs)", HEARTBEAT_INTERVAL)
    while True:
        try:
            await redis_ipc.cache_set(
                AGENT_HEARTBEAT_KEY,
                str(time.time()),
                ttl_seconds=90,
            )

            async with _registry_lock:
                for bot_id, entry in PROCESS_REGISTRY.items():
                    if entry.process.returncode is None:
                        await _publish_bot_status(
                            bot_id,
                            entry.status.value,
                            pid=entry.pid,
                            uptime=round(time.time() - entry.spawned_at, 1),
                        )

            await _sync_registry_to_redis()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Heartbeat error: %s", e)

        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def _spawn_active_fleet() -> list[str]:
    rows = await database.fetch(
        "SELECT bot_id FROM bots WHERE is_active = TRUE"
    )

    spawned = []
    for row in rows:
        bid = str(row["bot_id"])
        try:
            await spawn_bot(bid)
            spawned.append(bid)
        except RuntimeError as e:
            logger.warning("Skip spawn: %s", e)
        except Exception as e:
            logger.error("Failed to spawn %s: %s", bid[:8], e)

    logger.info("Active fleet spawned: %d/%d bots", len(spawned), len(rows))
    return spawned


async def _terminate_all() -> None:
    async with _registry_lock:
        bot_ids = list(PROCESS_REGISTRY.keys())

    if not bot_ids:
        return

    logger.info("Terminating all bots: %d processes", len(bot_ids))

    tasks = [
        asyncio.create_task(terminate_bot(bid), name=f"term-{bid[:8]}")
        for bid in bot_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for bid, result in zip(bot_ids, results):
        if isinstance(result, Exception):
            logger.error("Terminate error for %s: %s", bid[:8], result)


async def agent_main() -> None:
    logger.info("=== Two Moon AlmaLinux Agent — Booting ===")

    logger.info("[1/3] Connecting to CockroachDB...")
    await database.get_pool()
    logger.info("[1/3] CockroachDB ready")

    logger.info("[2/3] Connecting to Redis...")
    await redis_ipc.get_redis()
    logger.info("[2/3] Redis ready")

    logger.info("[3/3] Spawning active fleet from database...")
    spawned = await _spawn_active_fleet()
    logger.info("[3/3] Fleet online: %d bot(s)", len(spawned))

    await redis_ipc.subscribe_to_channel(
        FLEET_COMMAND_CHANNEL,
        _handle_fleet_command,
    )
    logger.info("Subscribed to fleet command channel")

    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(),
        name="agent-heartbeat",
    )

    logger.info("=== Two Moon AlmaLinux Agent — ONLINE ===")

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler(sig):
        logger.info("Received %s, initiating shutdown", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler, sig)

    await shutdown_event.wait()

    logger.info("=== Two Moon AlmaLinux Agent — Shutting Down ===")

    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass

    await _terminate_all()
    await redis_ipc.shutdown()
    await database.close_pool()

    logger.info("=== Two Moon AlmaLinux Agent — OFFLINE ===")


if __name__ == "__main__":
    asyncio.run(agent_main())
