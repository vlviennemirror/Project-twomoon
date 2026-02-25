import asyncio
import argparse
import logging
import os
import signal
import sys
from base64 import urlsafe_b64decode, urlsafe_b64encode
from pathlib import Path
from typing import Any, Optional

import uvloop
import discord
from discord.ext import commands

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared_lib import database
from shared_lib import redis_ipc

uvloop.install()

LOG_FORMAT = "[%(asctime)s] [%(name)-20s] [%(levelname)-7s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(bot_id: str) -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stdout,
    )
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.client").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


logger = logging.getLogger("twomoon.core_node")


COG_REGISTRY: dict[str, list[str]] = {
    "CORE_NODE": [
        "core_node.cogs.leveling",
        "core_node.cogs.moderation",
        "core_node.cogs.faction",
        "core_node.cogs.vouch",
    ],
    "MODERATION": [
        "core_node.cogs.moderation",
    ],
    "LEVELING": [
        "core_node.cogs.leveling",
        "core_node.cogs.faction",
    ],
    "CHATBOT": [
        "core_node.cogs.chatbot",
    ],
}


def decrypt_token(cipher_text: str, master_key: str) -> str:
    try:
        key_bytes = master_key.encode("utf-8")
        if len(key_bytes) != 44:
            key_bytes = urlsafe_b64encode(key_bytes.ljust(32, b"\0")[:32])
        from cryptography.fernet import Fernet

        f = Fernet(key_bytes)
        return f.decrypt(cipher_text.encode("utf-8")).decode("utf-8")
    except Exception:
        logger.warning("Fernet decryption unavailable, attempting raw base64 fallback")
        return urlsafe_b64decode(cipher_text + "==").decode("utf-8")


class CoreNode(commands.Bot):

    def __init__(self, bot_id: str, **kwargs: Any):
        self.bot_id = bot_id
        self.bot_type: str = "CORE_NODE"
        self.guild_id: Optional[str] = None
        self.config_cache: dict[str, Any] = {}
        self.moderation_rules_cache: list[dict[str, Any]] = []
        self._shutdown_event = asyncio.Event()

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

    async def setup_hook(self) -> None:
        logger.info("Setup hook initiated for bot_id=%s", self.bot_id)

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
            "Setup complete. Type=%s | Cogs=%d | Config keys=%d",
            self.bot_type,
            len(self.cogs),
            len(self.config_cache),
        )

    async def _load_bot_identity(self) -> None:
        row = await database.fetchrow(
            "SELECT guild_id, bot_type FROM bots WHERE bot_id = $1 AND is_active = TRUE",
            self.bot_id,
        )
        if not row:
            raise RuntimeError(f"Bot {self.bot_id} not found or inactive in fleet registry")

        self.guild_id = row["guild_id"]
        self.bot_type = row["bot_type"]
        logger.info("Identity loaded: type=%s, guild=%s", self.bot_type, self.guild_id)

    async def _hydrate_config(self) -> None:
        row = await database.fetchrow(
            """
            SELECT
                openrouter_api_key,
                ai_system_prompt,
                ai_model_id,
                log_channel_id,
                feature_flags,
                moderation_config,
                updated_at
            FROM bot_configs
            WHERE bot_id = $1 AND guild_id = $2
            """,
            self.bot_id,
            self.guild_id,
        )

        if row:
            self.config_cache = dict(row)
            logger.info(
                "Config hydrated from DB (updated_at=%s)",
                self.config_cache.get("updated_at"),
            )
        else:
            self.config_cache = {}
            logger.warning("No config row found for bot_id=%s, running with defaults", self.bot_id)

        guild_row = await database.fetchrow(
            "SELECT * FROM guild_settings WHERE guild_id = $1",
            self.guild_id,
        )
        if guild_row:
            self.config_cache["guild_settings"] = dict(guild_row)
            logger.info("Guild settings hydrated for guild %s", self.guild_id)

    async def _hydrate_moderation_rules(self) -> None:
        rows = await database.fetch(
            """
            SELECT rule_id, rule_name, rule_type, pattern, punishment_tier,
                   strike_duration_sec, sort_order
            FROM moderation_rules
            WHERE bot_id = $1 AND guild_id = $2 AND is_enabled = TRUE
            ORDER BY sort_order ASC
            """,
            self.bot_id,
            self.guild_id,
        )
        self.moderation_rules_cache = [dict(r) for r in rows]
        logger.info("Moderation rules hydrated: %d active rules", len(self.moderation_rules_cache))

    async def _start_ipc_listener(self) -> None:
        config_channel = redis_ipc.build_config_channel(self.bot_id)
        await redis_ipc.subscribe_to_channel(config_channel, self._on_ipc_event)

        if self.guild_id:
            guild_channel = redis_ipc.build_guild_channel(self.guild_id, "settings")
            await redis_ipc.subscribe_to_channel(guild_channel, self._on_ipc_event)

        logger.info("IPC listeners active")

    async def _on_ipc_event(self, event_type: str, payload: dict) -> None:
        logger.info("IPC event received: [%s] payload_keys=%s", event_type, list(payload.keys()))

        if event_type == "CONFIG_INVALIDATION":
            old_config = self.config_cache.copy()
            await self._hydrate_config()
            self.dispatch("config_reloaded", old_config, self.config_cache)
            logger.info("Hot-reload complete: config cache refreshed")

        elif event_type == "RULES_INVALIDATION":
            old_rules = self.moderation_rules_cache.copy()
            await self._hydrate_moderation_rules()
            self.dispatch("rules_reloaded", old_rules, self.moderation_rules_cache)
            logger.info("Hot-reload complete: moderation rules refreshed")

        elif event_type == "GUILD_SETTINGS_INVALIDATION":
            old_config = self.config_cache.copy()
            await self._hydrate_config()
            self.dispatch("guild_settings_reloaded", old_config, self.config_cache)
            logger.info("Hot-reload complete: guild settings refreshed")

        elif event_type == "COG_RELOAD":
            cog_name = payload.get("cog_name")
            if cog_name:
                await self._reload_single_cog(cog_name)

        elif event_type == "GRACEFUL_SHUTDOWN":
            logger.info("Remote shutdown command received via IPC")
            self._shutdown_event.set()
            await self.close()

    async def _load_cogs(self) -> None:
        cog_modules = COG_REGISTRY.get(self.bot_type, COG_REGISTRY["CORE_NODE"])

        for module_path in cog_modules:
            try:
                await self.load_extension(module_path)
                logger.info("Cog loaded: %s", module_path)
            except commands.ExtensionNotFound:
                logger.warning("Cog not found (skipped): %s", module_path)
            except commands.ExtensionFailed as e:
                logger.error("Cog failed to load: %s → %s", module_path, e)
            except Exception as e:
                logger.error("Unexpected error loading cog %s: %s", module_path, e)

    async def _reload_single_cog(self, cog_name: str) -> None:
        try:
            await self.reload_extension(cog_name)
            logger.info("Cog hot-reloaded via IPC: %s", cog_name)
            self.dispatch("cog_reloaded", cog_name)
        except commands.ExtensionNotLoaded:
            try:
                await self.load_extension(cog_name)
                logger.info("Cog loaded fresh via IPC: %s", cog_name)
            except Exception as e:
                logger.error("Failed to load cog %s via IPC: %s", cog_name, e)
        except Exception as e:
            logger.error("Failed to reload cog %s via IPC: %s", cog_name, e)

    async def on_ready(self) -> None:
        logger.info(
            "Core Node ONLINE | %s | Guilds: %d | Latency: %.0fms",
            self.user,
            len(self.guilds),
            self.latency * 1000,
        )

    async def close(self) -> None:
        logger.info("Graceful shutdown initiated")

        for cog_name, cog in list(self.cogs.items()):
            if hasattr(cog, "emergency_flush"):
                try:
                    await cog.emergency_flush()
                    logger.info("Emergency flush completed for cog: %s", cog_name)
                except Exception as e:
                    logger.error("Emergency flush failed for %s: %s", cog_name, e)

        await redis_ipc.shutdown()
        await database.close_pool()

        await super().close()
        logger.info("Core Node shutdown complete")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Two Moon Core Node — Hollow Engine")
    parser.add_argument(
        "--bot-id",
        type=str,
        required=True,
        help="UUID of the bot instance from the fleet registry",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_arguments()
    bot_id = args.bot_id

    configure_logging(bot_id)
    logger.info("Bootstrapping Core Node for bot_id=%s", bot_id)

    pool = await database.get_pool()

    row = await database.fetchrow(
        "SELECT token_cipher, is_active FROM bots WHERE bot_id = $1",
        bot_id,
    )

    if not row:
        logger.critical("FATAL: bot_id=%s does not exist in fleet registry", bot_id)
        await database.close_pool()
        sys.exit(1)

    if not row["is_active"]:
        logger.critical("FATAL: bot_id=%s is marked inactive", bot_id)
        await database.close_pool()
        sys.exit(1)

    master_key = os.environ.get("TWOMOON_MASTER_KEY", "")
    if not master_key:
        logger.critical("FATAL: TWOMOON_MASTER_KEY environment variable not set")
        await database.close_pool()
        sys.exit(1)

    token = decrypt_token(row["token_cipher"], master_key)
    logger.info("Token decrypted successfully for bot_id=%s", bot_id)

    bot = CoreNode(bot_id=bot_id)

    loop = asyncio.get_running_loop()

    def _signal_handler(sig: signal.Signals) -> None:
        logger.info("Received signal %s, initiating graceful shutdown", sig.name)
        asyncio.ensure_future(bot.close())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler, sig)

    try:
        async with bot:
            await bot.start(token)
    except discord.LoginFailure:
        logger.critical("FATAL: Discord rejected the bot token for bot_id=%s", bot_id)
        sys.exit(1)
    except Exception as e:
        logger.critical("FATAL: Unhandled exception during bot lifecycle: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        if not bot.is_closed():
            await bot.close()
        logger.info("Process exiting for bot_id=%s", bot_id)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
