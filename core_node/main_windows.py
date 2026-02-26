import asyncio
import ctypes
import ctypes.wintypes
import gc
import json
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any, Optional

_CORE_NODE_DIR = Path(__file__).resolve().parent
_ROOT          = _CORE_NODE_DIR.parent
sys.path.insert(0, str(_ROOT))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

gc.set_threshold(300, 8, 8)
gc.enable()

from dotenv import load_dotenv
load_dotenv()

import argparse

import discord
from discord.ext import commands

from shared_lib import database, redis_ipc
from shared_lib.encryption import decrypt_token

IPC_DIR = _ROOT / ".ipc"

_LOG_FORMAT = "[%(asctime)s] [%(name)-20s] [%(levelname)-7s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(bot_id: str) -> None:
    level    = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_file = _ROOT / f"bot_{bot_id[:8]}.log"

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=3 * 1024 * 1024,
                backupCount=1,
                encoding="utf-8",
            ),
        ],
    )
    for ns in ("discord.gateway", "discord.http", "discord.client", "asyncio"):
        logging.getLogger(ns).setLevel(logging.WARNING)


logger = logging.getLogger("twomoon.core_node_win")

COG_REGISTRY: dict[str, list[str]] = {
    "CORE_NODE": [
        "core_node.cogs.leveling",
        "core_node.cogs.apostle",
        "core_node.cogs.faction",
        "core_node.cogs.vouch",
    ],
    "MODERATION": ["core_node.cogs.apostle"],
    "LEVELING":   ["core_node.cogs.leveling", "core_node.cogs.faction"],
    "CHATBOT":    ["core_node.cogs.chatbot"],
}


_CTRL_HANDLER_REF: Optional[Any] = None


def _install_bot_ctrl_handler(on_shutdown_fn: Any) -> None:
    """
    Installs a Win32 Ctrl handler for the bot subprocess.
    `on_shutdown_fn` must be thread-safe (fires on OS signal thread).
    """
    global _CTRL_HANDLER_REF
    if sys.platform != "win32":
        return

    kernel32 = ctypes.windll.kernel32
    _HandlerRoutine = ctypes.WINFUNCTYPE(ctypes.wintypes.BOOL, ctypes.wintypes.DWORD)
    kernel32.SetConsoleCtrlHandler.restype  = ctypes.wintypes.BOOL
    kernel32.SetConsoleCtrlHandler.argtypes = [_HandlerRoutine, ctypes.wintypes.BOOL]

    CTRL_C_EVENT     = 0
    CTRL_BREAK_EVENT = 1
    CTRL_CLOSE_EVENT = 2

    @_HandlerRoutine
    def _handler(ctrl_type: int) -> bool:
        if ctrl_type in (CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT):
            on_shutdown_fn()
            return True
        return False

    _CTRL_HANDLER_REF = _handler
    ok = kernel32.SetConsoleCtrlHandler(_handler, True)
    if not ok:
        logger.warning(
            "SetConsoleCtrlHandler failed: error=%d", ctypes.get_last_error()
        )
    else:
        logger.debug("Bot Ctrl handler installed")


class LocalIPCServer:
    """
    Manages the bot's local IPC server lifecycle.
    Created in CoreNode.setup_hook(); torn down in CoreNode.close().
    """

    def __init__(self, bot_id: str, shutdown_callback: Any) -> None:
        self.bot_id            = bot_id
        self._shutdown_callback = shutdown_callback
        self._server: Optional[asyncio.Server] = None
        self._port:   Optional[int]            = None
        self._port_file = IPC_DIR / f"{bot_id}.port"

    @property
    def port(self) -> Optional[int]:
        return self._port

    async def start(self) -> None:
        """Bind server, record port, write port file."""
        IPC_DIR.mkdir(parents=True, exist_ok=True)
        self._server = await asyncio.start_server(
            self._handle_connection,
            host="127.0.0.1",
            port=0,
        )
        self._port = self._server.sockets[0].getsockname()[1]

        self._port_file.write_text(str(self._port), encoding="ascii")
        logger.info(
            "Local IPC server listening on 127.0.0.1:%d (port file: %s)",
            self._port, self._port_file,
        )

    async def stop(self) -> None:
        """
        Close the server and remove the port file.
        Called from CoreNode.close() before the process exits.
        """
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            self._port_file.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Failed to remove IPC port file: %s", exc)
        logger.info("Local IPC server stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Handles one incoming IPC connection.
        Expects exactly one JSON command per connection, responds, then closes.
        """
        peer = writer.get_extra_info("peername", ("?", 0))
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw:
                logger.warning("[IPC] Empty read from %s", peer)
                return

            try:
                message = json.loads(raw.decode("utf-8").strip())
            except json.JSONDecodeError as exc:
                logger.warning("[IPC] Malformed JSON from %s: %s", peer, exc)
                writer.write(
                    json.dumps({"status": "error", "msg": "invalid json"}).encode() + b"\n"
                )
                await writer.drain()
                return

            cmd = message.get("cmd", "").lower()

            if cmd == "shutdown":
                reason = message.get("reason", "unknown")
                logger.info("[IPC] Shutdown command received (reason=%s)", reason)

                writer.write(json.dumps({"status": "ok"}).encode() + b"\n")
                await writer.drain()

                asyncio.create_task(
                    self._shutdown_callback(),
                    name="ipc-local-shutdown",
                )

            elif cmd == "ping":
                writer.write(json.dumps({"status": "pong"}).encode() + b"\n")
                await writer.drain()

            else:
                logger.warning("[IPC] Unknown command '%s' from %s", cmd, peer)
                writer.write(
                    json.dumps({"status": "error", "msg": f"unknown cmd: {cmd}"}).encode()
                    + b"\n"
                )
                await writer.drain()

        except asyncio.TimeoutError:
            logger.warning("[IPC] Read timeout from %s", peer)
        except Exception as exc:
            logger.error("[IPC] Handler error from %s: %s", peer, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass


class CoreNode(commands.Bot):

    def __init__(self, bot_id: str, **kwargs: Any) -> None:
        self.bot_id     = bot_id
        self.bot_type:  str = "CORE_NODE"
        self.guild_id:  Optional[str] = None
        self.config_cache:            dict[str, Any]      = {}
        self.moderation_rules_cache:  list[dict[str, Any]] = []
        self._shutdown_event  = asyncio.Event()
        self._ipc_server:     Optional[LocalIPCServer]    = None
        self._closing         = False

        intents = discord.Intents(
            guilds=True,
            members=True,
            messages=True,
            message_content=True,
            voice_states=True,
            moderation=True,
            reactions=True,
        )
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
            max_messages=128,
            chunk_guilds_at_startup=False,
            member_cache_flags=discord.MemberCacheFlags.from_intents(intents),
            **kwargs,
        )

    async def _graceful_close_from_ipc(self) -> None:
        """
        Called by LocalIPCServer when a shutdown command is received.
        Schedules bot.close() without blocking the IPC handler coroutine.
        """
        if not self._closing:
            logger.info("IPC-triggered shutdown: calling bot.close()")
            await self.close()

    async def _start_local_ipc_server(self) -> None:
        self._ipc_server = LocalIPCServer(
            bot_id=self.bot_id,
            shutdown_callback=self._graceful_close_from_ipc,
        )
        await self._ipc_server.start()


    async def setup_hook(self) -> None:
        logger.info("Setup hook initiated for bot_id=%s", self.bot_id)

        await self._start_local_ipc_server()

        await self._load_bot_identity()
        await self._hydrate_config()
        await self._hydrate_moderation_rules()
        await self._start_ipc_listener()
        await self._load_cogs()

        if self.guild_id:
            guild_obj = discord.Object(id=int(self.guild_id))
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
            logger.info("Slash commands synced to guild %s", self.guild_id)

        logger.info(
            "Setup complete: type=%s | guild=%s | cogs=%d | config_keys=%d",
            self.bot_type, self.guild_id, len(self.cogs), len(self.config_cache),
        )


    async def _load_bot_identity(self) -> None:
        row = await database.fetchrow(
            "SELECT guild_id, bot_type FROM bots WHERE bot_id = $1 AND is_active = TRUE",
            self.bot_id,
        )
        if not row:
            raise RuntimeError(
                f"Bot {self.bot_id} not found or inactive in fleet registry"
            )
        self.guild_id = row["guild_id"]
        self.bot_type = row["bot_type"]
        logger.info(
            "Identity loaded: type=%s guild=%s", self.bot_type, self.guild_id
        )

    async def _hydrate_config(self) -> None:
        row = await database.fetchrow(
            """
            SELECT openrouter_api_key, ai_system_prompt, ai_model_id,
                   log_channel_id, feature_flags, moderation_config, updated_at
            FROM bot_configs
            WHERE bot_id = $1 AND guild_id = $2
            """,
            self.bot_id, self.guild_id,
        )
        self.config_cache = dict(row) if row else {}

        guild_row = await database.fetchrow(
            "SELECT * FROM guild_settings WHERE guild_id = $1", self.guild_id,
        )
        if guild_row:
            self.config_cache["guild_settings"] = dict(guild_row)

        logger.info(
            "Config hydrated: %d keys (updated_at=%s)",
            len(self.config_cache),
            self.config_cache.get("updated_at", "N/A"),
        )

    async def _hydrate_moderation_rules(self) -> None:
        rows = await database.fetch(
            """
            SELECT rule_id, rule_name, rule_type, pattern, punishment_tier,
                   strike_duration_sec, sort_order
            FROM moderation_rules
            WHERE bot_id = $1 AND guild_id = $2 AND is_enabled = TRUE
            ORDER BY sort_order ASC
            """,
            self.bot_id, self.guild_id,
        )
        self.moderation_rules_cache = [dict(r) for r in rows]
        logger.info(
            "Moderation rules hydrated: %d active rules",
            len(self.moderation_rules_cache),
        )


    async def _start_ipc_listener(self) -> None:
        """
        Subscribes to Redis Pub/Sub for config hot-reload signals.

        Note: This is NOT the graceful shutdown channel.  Shutdown is handled
        exclusively by LocalIPCServer (Mandate 1).  Redis Pub/Sub is used only
        for CONFIG_INVALIDATION, RULES_INVALIDATION, etc. — events that are
        recoverable if a message is dropped during a WiFi outage.
        """
        await redis_ipc.subscribe_to_channel(
            redis_ipc.build_config_channel(self.bot_id), self._on_redis_ipc_event,
        )
        if self.guild_id:
            await redis_ipc.subscribe_to_channel(
                redis_ipc.build_guild_channel(self.guild_id, "settings"),
                self._on_redis_ipc_event,
            )
        logger.info("Redis IPC listeners active")

    async def _on_redis_ipc_event(self, event_type: str, payload: dict) -> None:
        logger.info("Redis IPC event: [%s] keys=%s", event_type, list(payload.keys()))

        if event_type == "CONFIG_INVALIDATION":
            old = self.config_cache.copy()
            await self._hydrate_config()
            self.dispatch("config_reloaded", old, self.config_cache)
            logger.info("Hot-reload: config cache refreshed")

        elif event_type == "RULES_INVALIDATION":
            old = self.moderation_rules_cache.copy()
            await self._hydrate_moderation_rules()
            self.dispatch("rules_reloaded", old, self.moderation_rules_cache)
            logger.info("Hot-reload: moderation rules refreshed")

        elif event_type == "GUILD_SETTINGS_INVALIDATION":
            old = self.config_cache.copy()
            await self._hydrate_config()
            self.dispatch("guild_settings_reloaded", old, self.config_cache)
            logger.info("Hot-reload: guild settings refreshed")

        elif event_type == "COG_RELOAD":
            cog_name = payload.get("cog_name")
            if cog_name:
                await self._reload_single_cog(cog_name)

        elif event_type == "GRACEFUL_SHUTDOWN":
            logger.info(
                "GRACEFUL_SHUTDOWN received via Redis (legacy path). "
                "Normal path is Local IPC. Initiating close()."
            )
            if not self._closing:
                asyncio.create_task(self.close(), name="redis-legacy-shutdown")


    async def _load_cogs(self) -> None:
        cog_list = COG_REGISTRY.get(self.bot_type, COG_REGISTRY["CORE_NODE"])
        for module_path in cog_list:
            try:
                await self.load_extension(module_path)
                logger.info("Cog loaded: %s", module_path)
            except commands.ExtensionNotFound:
                logger.warning("Cog not found (skipped): %s", module_path)
            except commands.ExtensionFailed as exc:
                logger.error("Cog failed to load: %s → %s", module_path, exc)
            except Exception as exc:
                logger.error("Unexpected cog error %s: %s", module_path, exc)

    async def _reload_single_cog(self, cog_name: str) -> None:
        try:
            await self.reload_extension(cog_name)
            logger.info("Cog hot-reloaded: %s", cog_name)
            self.dispatch("cog_reloaded", cog_name)
        except commands.ExtensionNotLoaded:
            try:
                await self.load_extension(cog_name)
                logger.info("Cog loaded fresh (was not loaded): %s", cog_name)
            except Exception as exc:
                logger.error("Fresh-load failed for %s: %s", cog_name, exc)
        except Exception as exc:
            logger.error("Hot-reload failed for %s: %s", cog_name, exc)


    async def on_ready(self) -> None:
        logger.info(
            "Core Node ONLINE | %s | Guilds: %d | Latency: %.0fms",
            self.user, len(self.guilds), self.latency * 1000,
        )


    async def close(self) -> None:
        """
        Full teardown sequence.

        Order:
          1. Guard against re-entrant calls (Ctrl+C AND IPC firing simultaneously).
          2. Emergency flush of write-behind caches (XP, etc.) in all cogs.
          3. Stop the Local IPC server and remove port file.
          4. Tear down Redis IPC subscriptions.
          5. Close asyncpg pool.
          6. Call discord.py super().close() — sends Gateway close frame.
        """
        if self._closing:
            logger.debug("close() called re-entrantly — ignoring duplicate")
            return
        self._closing = True
        self._shutdown_event.set()

        logger.info("Graceful shutdown initiated for bot_id=%s", self.bot_id)

        for cog_name, cog in list(self.cogs.items()):
            if hasattr(cog, "emergency_flush"):
                try:
                    await cog.emergency_flush()
                    logger.info("Emergency flush OK: %s", cog_name)
                except Exception as exc:
                    logger.error("Emergency flush failed for %s: %s", cog_name, exc)

        if self._ipc_server:
            await self._ipc_server.stop()

        await redis_ipc.shutdown()

        await database.close_pool()

        await super().close()

        logger.info("Core Node shutdown complete for bot_id=%s", self.bot_id)



def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Two Moon Core Node — Windows (Audit Rev 2)"
    )
    parser.add_argument(
        "--bot-id", type=str, required=True,
        help="UUID of the bot instance from the fleet registry",
    )
    return parser.parse_args()



async def _init_db_with_backoff(bot_id: str) -> None:
    """
    Initialises the asyncpg pool with exponential back-off.

    Pool sizing for Windows 4 GB / 1 GB budget:
      min_size=1: Do not pre-allocate idle connections.  Each asyncpg connection
                  holds ~50–80 KB Python heap + TCP socket kernel buffer.
      max_size=3: One bot, one admin burst, one spare for command handling.
                  CockroachDB Serverless meters simultaneous connections;
                  a tight ceiling also protects RU quota.
      statement_cache_size=0: Already enforced in shared_lib/database.py.
                  Prevents asyncpg from caching prepared statement handles on
                  each connection object — on a long-running bot, the default
                  cache grows to 1024 entries × connection count.

    shared_lib/database.py currently hardcodes min_size=2 / max_size=8.
    For Windows we need to override this.  We set environment variables BEFORE
    calling get_pool() so that a Windows-aware database.py can read them.
    Alternatively, patch database._pool directly after pool creation:
    """
    import asyncpg
    import ssl
    from shared_lib import database as _db_module

    if _db_module._pool is not None and not _db_module._pool._closed:
        logger.info("DB pool already initialised (reusing existing)")
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode    = ssl.CERT_NONE

    import random
    attempt   = 0
    delay     = 2.0
    max_delay = 60.0

    while True:
        try:
            pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=1,
                max_size=3,
                max_inactive_connection_lifetime=300.0,
                command_timeout=30.0,
                statement_cache_size=0,
                server_settings={
                    "application_name": f"twomoon_win_{bot_id[:8]}",
                },
                ssl=ssl_ctx,
            )
            _db_module._pool = pool
            logger.info(
                "CockroachDB pool ready (min=1, max=3, stmt_cache=0, bot=%s)",
                bot_id[:8],
            )
            return

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            wait = random.uniform(0, min(delay, max_delay))
            logger.warning(
                "DB connect attempt %d failed (%s: %s). Retrying in %.1fs...",
                attempt, type(exc).__name__, exc, wait,
            )
            await asyncio.sleep(wait)
            delay = min(delay * 2, max_delay)



async def async_main() -> None:
    args   = parse_arguments()
    bot_id = args.bot_id
    configure_logging(bot_id)

    logger.info("=" * 64)
    logger.info("  Core Node bootstrapping · bot_id=%s", bot_id)
    logger.info("  Python=%s · Platform=%s", sys.version.split()[0], sys.platform)
    logger.info("  GC thresholds: %s", gc.get_threshold())
    logger.info("=" * 64)

    await _init_db_with_backoff(bot_id)

    row = await database.fetchrow(
        "SELECT is_active FROM bots WHERE bot_id = $1", bot_id,
    )
    if not row:
        logger.critical("FATAL: bot_id=%s not found in fleet registry", bot_id)
        await database.close_pool()
        sys.exit(1)
    if not row["is_active"]:
        logger.critical("FATAL: bot_id=%s is marked inactive", bot_id)
        await database.close_pool()
        sys.exit(1)

    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        logger.critical("FATAL: DISCORD_TOKEN environment variable not set")
        await database.close_pool()
        sys.exit(1)

    bot = CoreNode(bot_id=bot_id)

    loop = asyncio.get_running_loop()
    _install_bot_ctrl_handler(
        on_shutdown_fn=lambda: loop.call_soon_threadsafe(
            loop.create_task,
            bot.close(),
        )
    )

    try:
        async with bot:
            await bot.start(token)
    except discord.LoginFailure:
        logger.critical("FATAL: Discord rejected token for bot_id=%s", bot_id)
        sys.exit(1)
    except discord.PrivilegedIntentsRequired as exc:
        logger.critical("FATAL: Missing privileged intents: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.critical(
            "FATAL: Unhandled exception in bot lifecycle: %s", exc, exc_info=True,
        )
        sys.exit(1)
    finally:
        if not bot.is_closed():
            await bot.close()
        logger.info("Process exiting for bot_id=%s", bot_id)



def main() -> None:
    if sys.platform == "win32":
        try:
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:
            pass

    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Interrupted before event loop fully started. Exiting cleanly.")

    sys.exit(0)


if __name__ == "__main__":
    main()