import asyncio
import ctypes
import ctypes.wintypes
import gc
import json
import logging
import logging.handlers
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False
    print("[WARN] psutil not installed — memory watchdog disabled. `pip install psutil`")

from shared_lib import database, redis_ipc

gc.set_threshold(300, 8, 8)
gc.enable()

_LOG_FORMAT = "[%(asctime)s] [%(name)-24s] [%(levelname)-7s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=_LOG_FORMAT,
    datefmt=_DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            _ROOT / "twomoon_agent.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=2,
            encoding="utf-8",
        ),
    ],
)
for _ns in ("asyncio", "discord.gateway", "discord.http", "aiohttp.access"):
    logging.getLogger(_ns).setLevel(logging.WARNING)

logger = logging.getLogger("twomoon.windows_agent")

FLEET_COMMAND_CHANNEL = "events:fleet_command"
FLEET_STATUS_CHANNEL  = "events:fleet_status"
AGENT_HEARTBEAT_KEY   = "fleet:agent:heartbeat"
BOT_STATUS_KEY_PREFIX = "fleet:status:"
FLEET_REGISTRY_KEY    = "fleet:registry"
BOT_STATUS_TTL        = 120

CORE_NODE_SCRIPT  = str(_ROOT / "core_node" / "main_windows.py")
PYTHON_EXECUTABLE = os.environ.get("TWOMOON_PYTHON", sys.executable)

IPC_DIR = _ROOT / ".ipc"

LOCAL_IPC_CONNECT_TIMEOUT = 3.0
LOCAL_IPC_ACK_TIMEOUT     = 5.0
GRACEFUL_TIMEOUT          = 12.0
JOB_CLOSE_TIMEOUT         = 4.0
HEARTBEAT_INTERVAL        = 30.0
BOT_RSS_BUDGET_MB         = int(os.environ.get("BOT_RSS_BUDGET_MB", "400"))


kernel32 = ctypes.windll.kernel32 if sys.platform == "win32" else None

JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE   = 0x00002000
JOB_OBJECT_INFO_CLASS_EXTENDED_LIMIT  = 9
PROCESS_ALL_ACCESS                    = 0x1F0FFF
INVALID_HANDLE_VALUE                  = ctypes.c_void_p(-1).value


class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
    """
    JOBOBJECT_BASIC_LIMIT_INFORMATION from WinNT.h — 64-byte layout on Windows x64.

    Field mapping (Windows x64 — c_ulong is 4 bytes, c_size_t is 8 bytes):
      PerProcessUserTimeLimit : LARGE_INTEGER → c_longlong (8)
      PerJobUserTimeLimit     : LARGE_INTEGER → c_longlong (8)
      LimitFlags              : DWORD         → c_ulong    (4) [+4 pad]
      MinimumWorkingSetSize   : SIZE_T        → c_size_t   (8)
      MaximumWorkingSetSize   : SIZE_T        → c_size_t   (8)
      ActiveProcessLimit      : DWORD         → c_ulong    (4) [+4 pad]
      Affinity                : ULONG_PTR     → c_size_t   (8)
      PriorityClass           : DWORD         → c_ulong    (4)
      SchedulingClass         : DWORD         → c_ulong    (4)
    Total on Windows = 64 bytes.
    """
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit",     ctypes.c_longlong),
        ("LimitFlags",              ctypes.c_ulong),
        ("MinimumWorkingSetSize",   ctypes.c_size_t),
        ("MaximumWorkingSetSize",   ctypes.c_size_t),
        ("ActiveProcessLimit",      ctypes.c_ulong),
        ("Affinity",                ctypes.c_size_t),
        ("PriorityClass",           ctypes.c_ulong),
        ("SchedulingClass",         ctypes.c_ulong),
    ]


class _IO_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount",  ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount",   ctypes.c_ulonglong),
        ("WriteTransferCount",  ctypes.c_ulonglong),
        ("OtherTransferCount",  ctypes.c_ulonglong),
    ]


class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    """
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION from WinNT.h — 112 bytes on Windows x64.
    """
    _fields_ = [
        ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
        ("IoInfo",                _IO_COUNTERS),
        ("ProcessMemoryLimit",    ctypes.c_size_t),
        ("JobMemoryLimit",        ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed",     ctypes.c_size_t),
    ]


def _create_kill_on_close_job() -> Optional[int]:
    """
    Creates a Windows Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE set.

    Returns the raw HANDLE integer on success, or None on failure.
    Caller MUST store this handle and later call _close_job_handle() to trigger
    the kill-on-close guarantee (or to clean up after natural process exit).
    """
    if kernel32 is None:
        return None

    kernel32.CreateJobObjectW.restype  = ctypes.wintypes.HANDLE
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    kernel32.SetInformationJobObject.restype  = ctypes.wintypes.BOOL
    kernel32.SetInformationJobObject.argtypes = [
        ctypes.wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_ulong,
    ]

    job_handle = kernel32.CreateJobObjectW(None, None)
    if not job_handle or job_handle == INVALID_HANDLE_VALUE:
        logger.error("CreateJobObjectW failed: error=%d", ctypes.get_last_error())
        return None

    info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    ctypes.memset(ctypes.byref(info), 0, ctypes.sizeof(info))
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

    ok = kernel32.SetInformationJobObject(
        job_handle,
        JOB_OBJECT_INFO_CLASS_EXTENDED_LIMIT,
        ctypes.byref(info),
        ctypes.c_ulong(ctypes.sizeof(info)),
    )
    if not ok:
        logger.error("SetInformationJobObject failed: error=%d", ctypes.get_last_error())
        kernel32.CloseHandle(job_handle)
        return None

    return int(job_handle)


def _assign_process_to_job(job_handle: int, pid: int) -> bool:
    """
    Opens the target process by PID and assigns it to the Job Object.
    Immediately closes the per-process handle after assignment; the Job Object
    handle (stored in ProcessEntry) is the only one that must remain open.
    """
    if kernel32 is None:
        return False

    kernel32.OpenProcess.restype  = ctypes.wintypes.HANDLE
    kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.wintypes.BOOL, ctypes.c_ulong]
    kernel32.AssignProcessToJobObject.restype  = ctypes.wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [
        ctypes.wintypes.HANDLE, ctypes.wintypes.HANDLE,
    ]
    kernel32.CloseHandle.restype  = ctypes.wintypes.BOOL
    kernel32.CloseHandle.argtypes = [ctypes.wintypes.HANDLE]

    proc_handle = kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, pid)
    if not proc_handle or proc_handle == INVALID_HANDLE_VALUE:
        logger.error("OpenProcess(pid=%d) failed: error=%d", pid, ctypes.get_last_error())
        return False

    ok = kernel32.AssignProcessToJobObject(job_handle, proc_handle)
    if not ok:
        err = ctypes.get_last_error()
        logger.warning(
            "AssignProcessToJobObject(pid=%d) failed: error=%d "
            "(already in non-extensible job? Kill-on-close unavailable for this run.)",
            pid, err,
        )

    kernel32.CloseHandle(proc_handle)
    return bool(ok)


def _close_job_handle(job_handle: Optional[int]) -> None:
    """
    Closes the Job Object handle.

    Per JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE semantics: closing the last handle
    to the job causes the Windows kernel to immediately terminate ALL processes
    in the entire job tree, including grandchildren.  This is our Phase 2
    force-kill — it is unconditional and races no cleanup code.
    """
    if kernel32 is None or not job_handle:
        return
    kernel32.CloseHandle.restype  = ctypes.wintypes.BOOL
    kernel32.CloseHandle.argtypes = [ctypes.wintypes.HANDLE]
    kernel32.CloseHandle(ctypes.wintypes.HANDLE(job_handle))


_CTRL_HANDLER_REF: Optional[Any] = None


def install_console_ctrl_handler(on_shutdown: Callable[[], None]) -> None:
    """
    Registers a Win32 Ctrl handler that calls `on_shutdown()` on any
    termination signal.  `on_shutdown` MUST be thread-safe because Win32
    fires it on a dedicated OS signal-dispatcher thread, not the asyncio
    event loop thread.
    """
    global _CTRL_HANDLER_REF
    if kernel32 is None:
        return

    _HandlerRoutine = ctypes.WINFUNCTYPE(ctypes.wintypes.BOOL, ctypes.wintypes.DWORD)
    kernel32.SetConsoleCtrlHandler.restype  = ctypes.wintypes.BOOL
    kernel32.SetConsoleCtrlHandler.argtypes = [_HandlerRoutine, ctypes.wintypes.BOOL]

    CTRL_C_EVENT     = 0
    CTRL_BREAK_EVENT = 1
    CTRL_CLOSE_EVENT = 2

    @_HandlerRoutine
    def _handler(ctrl_type: int) -> bool:
        if ctrl_type in (CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT):
            on_shutdown()
            return True
        return False

    _CTRL_HANDLER_REF = _handler
    result = kernel32.SetConsoleCtrlHandler(_handler, True)
    if not result:
        logger.warning("SetConsoleCtrlHandler failed: error=%d", ctypes.get_last_error())
    else:
        logger.debug("Console Ctrl handler installed")


def _ipc_port_file(bot_id: str) -> Path:
    return IPC_DIR / f"{bot_id}.port"


async def send_local_ipc_shutdown(bot_id: str, reason: str = "agent_command") -> bool:
    """
    Sends {"cmd": "shutdown"} to the bot's loopback TCP server.

    Returns True  if the bot acknowledged with {"status": "ok"}.
    Returns False if the port file is missing, the connection was refused,
                  or the ACK timed out.  Caller must then execute Phase 2.
    """
    port_file = _ipc_port_file(bot_id)
    if not port_file.exists():
        logger.warning(
            "[IPC] Port file not found for %s — bot may still be initializing "
            "or crashed before IPC server started",
            bot_id[:8],
        )
        return False

    try:
        port = int(port_file.read_text(encoding="ascii").strip())
    except (ValueError, OSError) as exc:
        logger.warning("[IPC] Cannot parse port file for %s: %s", bot_id[:8], exc)
        return False

    logger.info("[IPC] Connecting → 127.0.0.1:%d (bot=%s)", port, bot_id[:8])

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port),
            timeout=LOCAL_IPC_CONNECT_TIMEOUT,
        )
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as exc:
        logger.warning("[IPC] Cannot connect to bot %s: %s", bot_id[:8], exc)
        return False

    try:
        writer.write(
            json.dumps({"cmd": "shutdown", "reason": reason}).encode("utf-8") + b"\n"
        )
        await writer.drain()

        raw = await asyncio.wait_for(reader.readline(), timeout=LOCAL_IPC_ACK_TIMEOUT)
        if not raw:
            logger.warning("[IPC] Bot %s closed connection without ACK", bot_id[:8])
            return False

        response = json.loads(raw.decode("utf-8").strip())
        if response.get("status") == "ok":
            logger.info("[IPC] Shutdown ACK received from bot %s", bot_id[:8])
            return True
        logger.warning("[IPC] Unexpected response from %s: %s", bot_id[:8], response)
        return False

    except (asyncio.TimeoutError, json.JSONDecodeError, OSError) as exc:
        logger.warning("[IPC] Protocol error with bot %s: %s", bot_id[:8], exc)
        return False
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except OSError:
            pass



class BotStatus(str, Enum):
    STARTING = "STARTING"
    RUNNING  = "RUNNING"
    STOPPING = "STOPPING"
    EXITED   = "EXITED"
    CRASHED  = "CRASHED"


@dataclass
class ProcessEntry:
    bot_id:       str
    process:      asyncio.subprocess.Process
    monitor_task: asyncio.Task
    pid:          int
    job_handle:   Optional[int]
    status:       BotStatus    = BotStatus.STARTING
    spawned_at:   float        = field(default_factory=time.monotonic)
    exit_code:    Optional[int] = None


PROCESS_REGISTRY: dict[str, ProcessEntry] = {}
_registry_lock = asyncio.Lock()



async def _publish_bot_status(bot_id: str, status: str, **extra: Any) -> None:
    payload = {"bot_id": bot_id, "status": status, "ts": time.time(), **extra}
    try:
        await redis_ipc.cache_set_json(
            f"{BOT_STATUS_KEY_PREFIX}{bot_id}", payload, ttl_seconds=BOT_STATUS_TTL,
        )
        await redis_ipc.publish_event(FLEET_STATUS_CHANNEL, "BOT_STATUS_UPDATE", payload)
    except Exception as exc:
        logger.debug("Status publish skipped (Redis unavailable): %s", exc)


async def _sync_registry_to_redis() -> None:
    async with _registry_lock:
        bot_ids = list(PROCESS_REGISTRY.keys())
    try:
        await redis_ipc.cache_set_json(
            FLEET_REGISTRY_KEY, bot_ids, ttl_seconds=BOT_STATUS_TTL,
        )
    except Exception:
        pass


async def _drain_stream(
    stream: asyncio.StreamReader,
    bot_id: str,
    stream_name: str,
) -> None:
    """
    Actively drains a child process pipe to prevent ProactorEventLoop deadlock.

    Windows Proactor pipe buffers default to 65 KB.  A verbose bot (e.g., one
    logging raw WebSocket frames) fills that in seconds.  Once full, the OS
    suspends the child process indefinitely with zero error output — silent
    deadlock.  Active draining prevents this.
    """
    log_fn = logger.info if stream_name == "stdout" else logger.warning
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            if decoded:
                log_fn("[%s][%s] %s", bot_id[:8], stream_name, decoded)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error("Drain error %s/%s: %s", bot_id[:8], stream_name, exc)


async def _cancel_and_release_drain_tasks(*tasks: Optional[asyncio.Task]) -> None:
    """
    Cancels all live drain tasks and awaits them to completion.

    Using gather(return_exceptions=True) ensures every task reaches its terminal
    state and the ProactorEventLoop processes each IOCP cancellation callback
    before this function returns — preventing pipe handle leaks (see M4A).
    """
    live = [t for t in tasks if t is not None and not t.done()]
    if not live:
        return
    for t in live:
        t.cancel()
    await asyncio.gather(*live, return_exceptions=True)



async def _monitor_and_reap(bot_id: str, proc: asyncio.subprocess.Process) -> None:
    """
    Background task: drains I/O pipes, collects exit code, updates registry,
    and releases the Job Object handle.
    """
    stdout_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None

    try:
        if proc.stdout:
            stdout_task = asyncio.create_task(
                _drain_stream(proc.stdout, bot_id, "stdout"),
                name=f"drain-out-{bot_id[:8]}",
            )
        if proc.stderr:
            stderr_task = asyncio.create_task(
                _drain_stream(proc.stderr, bot_id, "stderr"),
                name=f"drain-err-{bot_id[:8]}",
            )

        exit_code = await proc.wait()

        live_at_exit = [t for t in (stdout_task, stderr_task) if t and not t.done()]
        if live_at_exit:
            _, pending = await asyncio.wait(live_at_exit, timeout=2.0)
            if pending:
                await _cancel_and_release_drain_tasks(*pending)

        async with _registry_lock:
            entry = PROCESS_REGISTRY.pop(bot_id, None)

        final_status = BotStatus.EXITED.value
        if entry:
            entry.exit_code = exit_code
            if entry.status == BotStatus.STOPPING or exit_code == 0:
                entry.status = BotStatus.EXITED
            else:
                entry.status = BotStatus.CRASHED
            final_status = entry.status.value
            uptime = time.monotonic() - entry.spawned_at
            logger.info(
                "Bot reaped: %s pid=%d exit=%d status=%s uptime=%.0fs",
                bot_id[:8], entry.pid, exit_code, final_status, uptime,
            )
            _close_job_handle(entry.job_handle)
        else:
            logger.warning(
                "Bot reaped but registry entry missing: %s pid=%d exit=%d",
                bot_id[:8], proc.pid, exit_code,
            )

        await _publish_bot_status(bot_id, final_status, exit_code=exit_code, pid=proc.pid)
        await _sync_registry_to_redis()

    except asyncio.CancelledError:
        await _cancel_and_release_drain_tasks(stdout_task, stderr_task)
        raise


async def spawn_bot(bot_id: str) -> ProcessEntry:
    async with _registry_lock:
        existing = PROCESS_REGISTRY.get(bot_id)
        if existing and existing.process.returncode is None:
            raise RuntimeError(f"Bot {bot_id[:8]} already running (pid={existing.pid})")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"]  = "1"
    env["PYTHONIOENCODING"]  = "utf-8"

    logger.info("Spawning bot: %s", bot_id[:8])

    proc = await asyncio.create_subprocess_exec(
        PYTHON_EXECUTABLE,
        CORE_NODE_SCRIPT,
        "--bot-id", bot_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=env,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )

    job_handle = _create_kill_on_close_job()
    if job_handle:
        assigned = _assign_process_to_job(job_handle, proc.pid)
        if assigned:
            logger.debug("Bot %s (pid=%d) bound to Job Object", bot_id[:8], proc.pid)
        else:
            _close_job_handle(job_handle)
            job_handle = None
    else:
        logger.warning(
            "Job Object unavailable for %s — process tree isolation not active",
            bot_id[:8],
        )

    entry = ProcessEntry(
        bot_id=bot_id,
        process=proc,
        monitor_task=asyncio.create_task(
            _monitor_and_reap(bot_id, proc),
            name=f"reaper-{bot_id[:8]}",
        ),
        pid=proc.pid,
        job_handle=job_handle,
        status=BotStatus.RUNNING,
    )

    async with _registry_lock:
        PROCESS_REGISTRY[bot_id] = entry

    logger.info(
        "Bot spawned: %s pid=%d job=%s", bot_id[:8], proc.pid, bool(job_handle)
    )
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

    logger.info("Phase 1: Local IPC shutdown → bot %s", bot_id[:8])
    ipc_ack = await send_local_ipc_shutdown(bot_id, reason="agent_terminate")

    if ipc_ack:
        try:
            exit_code = await asyncio.wait_for(
                entry.process.wait(), timeout=GRACEFUL_TIMEOUT,
            )
            logger.info(
                "Bot %s exited gracefully (Phase 1): code=%d", bot_id[:8], exit_code,
            )
            return exit_code
        except asyncio.TimeoutError:
            logger.warning(
                "Bot %s did not exit within %.0fs after IPC ACK. Escalating.",
                bot_id[:8], GRACEFUL_TIMEOUT,
            )
    else:
        logger.warning(
            "Phase 1 IPC unavailable for %s. Proceeding to Phase 2.",
            bot_id[:8],
        )

    logger.warning(
        "Phase 2: CloseHandle(job=%s) → kernel process-tree annihilation for %s",
        bool(entry.job_handle), bot_id[:8],
    )
    jh = entry.job_handle
    entry.job_handle = None
    _close_job_handle(jh)

    try:
        exit_code = await asyncio.wait_for(
            entry.process.wait(), timeout=JOB_CLOSE_TIMEOUT,
        )
        logger.info(
            "Bot %s annihilated (Phase 2): code=%d", bot_id[:8], exit_code,
        )
        return exit_code
    except asyncio.TimeoutError:
        logger.critical(
            "Bot %s (pid=%d) unresponsive after Job close. Calling TerminateProcess.",
            bot_id[:8], entry.pid,
        )
        try:
            entry.process.terminate()
            exit_code = await asyncio.wait_for(entry.process.wait(), timeout=3.0)
            return exit_code
        except (asyncio.TimeoutError, ProcessLookupError) as exc:
            logger.critical(
                "TerminateProcess failed for %s: %s — PID %d may be orphaned",
                bot_id[:8], exc, entry.pid,
            )
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
    await asyncio.sleep(2.0)
    return await spawn_bot(bot_id)



async def _handle_fleet_command(event_type: str, payload: dict) -> None:
    if event_type != "FLEET_COMMAND":
        return

    action       = payload.get("action", "").upper()
    bot_id       = payload.get("bot_id")
    requested_by = payload.get("requested_by", "unknown")

    if not bot_id:
        logger.warning("Fleet command missing bot_id: %s", payload)
        return

    logger.info(
        "Fleet command: action=%s bot=%s from=%s", action, bot_id[:8], requested_by,
    )

    try:
        if action == "START":
            await spawn_bot(bot_id)
        elif action == "STOP":
            await terminate_bot(bot_id)
        elif action == "RESTART":
            await restart_bot(bot_id)
        elif action == "STATUS":
            async with _registry_lock:
                entry = PROCESS_REGISTRY.get(bot_id)
            if entry:
                await _publish_bot_status(
                    bot_id, entry.status.value,
                    pid=entry.pid,
                    uptime=round(time.monotonic() - entry.spawned_at, 1),
                )
            else:
                await _publish_bot_status(bot_id, "NOT_RUNNING")
        else:
            logger.warning("Unknown fleet action: %s", action)

    except (RuntimeError, KeyError) as exc:
        logger.warning("Fleet command failed: %s", exc)
        await _publish_bot_status(bot_id, "ERROR", error=str(exc))
    except Exception as exc:
        logger.error("Fleet command unexpected error: %s", exc, exc_info=True)
        await _publish_bot_status(bot_id, "ERROR", error=str(exc))


async def _memory_watchdog_loop() -> None:
    if not _PSUTIL_AVAILABLE:
        logger.warning("Memory watchdog disabled — `pip install psutil` to enable RSS monitoring")
        return

    logger.info(
        "Memory watchdog started (interval=60s, bot_rss_budget=%d MB)", BOT_RSS_BUDGET_MB,
    )

    while True:
        try:
            await asyncio.sleep(60.0)

            async with _registry_lock:
                entries = list(PROCESS_REGISTRY.values())

            for entry in entries:
                if entry.process.returncode is not None:
                    continue
                try:
                    p = psutil.Process(entry.pid)
                    rss_mb = p.memory_info().rss / (1024 * 1024)
                    logger.debug("Bot %s: RSS=%.0f MB", entry.bot_id[:8], rss_mb)

                    if rss_mb > BOT_RSS_BUDGET_MB:
                        logger.critical(
                            "Bot %s RSS=%.0f MB exceeds budget=%d MB. "
                            "Scheduling graceful restart via Local IPC.",
                            entry.bot_id[:8], rss_mb, BOT_RSS_BUDGET_MB,
                        )
                        asyncio.create_task(
                            restart_bot(entry.bot_id),
                            name=f"oom-restart-{entry.bot_id[:8]}",
                        )

                except psutil.NoSuchProcess:
                    pass
                except Exception as exc:
                    logger.error("RSS check error for %s: %s", entry.bot_id[:8], exc)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Memory watchdog unhandled error: %s", exc)



async def _fleet_command_listener() -> None:
    import json
    r = await redis_ipc.get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("twomoon:fleet_commands")
    logger.info("Fleet command listener subscribed to twomoon:fleet_commands")
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                payload = json.loads(message["data"])
                command = payload.get("command", "").lower()
                bot_id = payload.get("bot_id", "")
                if not bot_id:
                    logger.warning("Fleet command missing bot_id: %s", payload)
                    continue
                logger.info("Fleet command received: command=%s bot=%s", command, bot_id[:8])
                if command == "start":
                    asyncio.create_task(spawn_bot(bot_id), name=f"cmd-start-{bot_id[:8]}")
                elif command == "stop":
                    asyncio.create_task(terminate_bot(bot_id), name=f"cmd-stop-{bot_id[:8]}")
                elif command == "restart":
                    asyncio.create_task(restart_bot(bot_id), name=f"cmd-restart-{bot_id[:8]}")
                else:
                    logger.warning("Unknown fleet command: %s", command)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning("Malformed fleet command payload: %s", exc)
            except Exception as exc:
                logger.error("Fleet command handler error: %s", exc, exc_info=True)
    except asyncio.CancelledError:
        await pubsub.unsubscribe("twomoon:fleet_commands")
        await pubsub.aclose()
        raise


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
                snapshot = list(PROCESS_REGISTRY.items())
            for bot_id, entry in snapshot:
                if entry.process.returncode is None:
                    await _publish_bot_status(
                        bot_id, entry.status.value,
                        pid=entry.pid,
                        uptime=round(time.monotonic() - entry.spawned_at, 1),
                    )
            await _sync_registry_to_redis()

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Heartbeat error: %s", exc)

        await asyncio.sleep(HEARTBEAT_INTERVAL)



async def _connect_with_backoff(
    coro_factory: Callable,
    label: str,
    max_attempts: int = 0,
) -> Any:
    """
    Calls `coro_factory()` with exponential back-off and full jitter.
    max_attempts=0 = retry forever (correct for infrastructure connections).
    Full jitter prevents thundering-herd when Railway services restart together.
    """
    import random
    attempt   = 0
    delay     = 2.0
    max_delay = 60.0

    while True:
        try:
            return await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            if max_attempts and attempt >= max_attempts:
                raise
            wait = random.uniform(0, min(delay, max_delay))
            logger.warning(
                "[%s] Attempt %d failed (%s: %s). Retrying in %.1fs...",
                label, attempt, type(exc).__name__, exc, wait,
            )
            await asyncio.sleep(wait)
            delay = min(delay * 2, max_delay)



async def _spawn_active_fleet() -> list[str]:
    rows = await database.fetch("SELECT bot_id FROM bots WHERE is_active = TRUE")
    spawned = []
    for row in rows:
        bid = str(row["bot_id"])
        try:
            await spawn_bot(bid)
            spawned.append(bid)
        except RuntimeError as exc:
            logger.warning("Skip spawn: %s", exc)
        except Exception as exc:
            logger.error("Failed to spawn %s: %s", bid[:8], exc)
    logger.info("Active fleet spawned: %d/%d bots", len(spawned), len(rows))
    return spawned


async def _terminate_all() -> None:
    async with _registry_lock:
        bot_ids = list(PROCESS_REGISTRY.keys())
    if not bot_ids:
        return
    logger.info("Terminating all bots: %d processes", len(bot_ids))
    results = await asyncio.gather(
        *(asyncio.create_task(terminate_bot(bid), name=f"term-{bid[:8]}") for bid in bot_ids),
        return_exceptions=True,
    )
    for bid, result in zip(bot_ids, results):
        if isinstance(result, Exception):
            logger.error("Terminate error for %s: %s", bid[:8], result)



async def agent_main() -> None:
    IPC_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 68)
    logger.info("  Two Moon Windows Agent · Audit Rev 2")
    logger.info("  PID=%d · Python=%s · Platform=%s", os.getpid(), sys.version.split()[0], sys.platform)
    logger.info("  GC thresholds : %s", gc.get_threshold())
    logger.info("  Job Objects   : %s", kernel32 is not None)
    logger.info("  psutil        : %s", _PSUTIL_AVAILABLE)
    logger.info("  IPC dir       : %s", IPC_DIR)
    logger.info("=" * 68)

    logger.info("[1/4] Connecting to CockroachDB...")
    await _connect_with_backoff(database.get_pool, "CockroachDB")

    logger.info("[2/4] Connecting to Redis...")
    await _connect_with_backoff(redis_ipc.get_redis, "Redis")

    logger.info("[3/4] Subscribing to fleet command channel...")
    await redis_ipc.subscribe_to_channel(FLEET_COMMAND_CHANNEL, _handle_fleet_command)

    logger.info("[4/4] Spawning active fleet from database...")
    spawned = await _spawn_active_fleet()
    logger.info("Fleet online: %d active bot(s)", len(spawned))

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    install_console_ctrl_handler(
        on_shutdown=lambda: loop.call_soon_threadsafe(shutdown_event.set)
    )

    background_tasks = [
        asyncio.create_task(_heartbeat_loop(),           name="agent-heartbeat"),
        asyncio.create_task(_memory_watchdog_loop(),     name="memory-watchdog"),
        asyncio.create_task(_fleet_command_listener(),   name="fleet-cmd-listener"),
    ]

    logger.info("=" * 68)
    logger.info("  Two Moon Windows Agent — ONLINE")
    logger.info("  Ctrl+C / Ctrl+Break / window close → graceful shutdown")
    logger.info("=" * 68)

    await shutdown_event.wait()

    logger.info("=" * 68)
    logger.info("  Two Moon Windows Agent — Shutting Down")
    logger.info("=" * 68)

    for task in background_tasks:
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)

    await _terminate_all()
    await redis_ipc.shutdown()
    await database.close_pool()

    logger.info("=" * 68)
    logger.info("  Two Moon Windows Agent — OFFLINE")
    logger.info("=" * 68)



if __name__ == "__main__":
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:
            pass

    try:
        asyncio.run(agent_main())
    except KeyboardInterrupt:
        logger.info("Interrupted before event loop fully started. Exiting cleanly.")

    sys.exit(0)