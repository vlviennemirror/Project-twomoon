import asyncio
import math
import random
import time
import logging
from dataclasses import dataclass
from typing import Any, Optional

import discord
from discord.ext import commands, tasks

from shared_lib import database
from shared_lib import redis_ipc

logger = logging.getLogger("twomoon.cogs.leveling")

FLUSH_INTERVAL_MINUTES = 5.0
BUFFER_COUNT_THRESHOLD = 100
DEFAULT_LEVEL_BASE = 100
DEFAULT_LEVEL_EXPONENT = 1.5
DEFAULT_MSG_XP_MIN = 15
DEFAULT_MSG_XP_MAX = 25
DEFAULT_MSG_COOLDOWN = 60
DEFAULT_REACT_XP = 5
DEFAULT_REACT_COOLDOWN = 30
DEFAULT_VOICE_XP_PER_MIN = 10
VOICE_MUTED_PENALTY = 0.5
ARTEMISIUM_LEVEL_GATE = 5
ARTEMISIUM_GRANT = 1

BATCH_UPSERT_SQL = """
    INSERT INTO user_levels
        (guild_id, user_id, faction, xp, level,
         total_messages, total_reactions, total_voice_minutes, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
    ON CONFLICT (guild_id, user_id, faction)
    DO UPDATE SET
        xp = user_levels.xp + EXCLUDED.xp,
        level = GREATEST(user_levels.level, EXCLUDED.level),
        total_messages = user_levels.total_messages + EXCLUDED.total_messages,
        total_reactions = user_levels.total_reactions + EXCLUDED.total_reactions,
        total_voice_minutes = user_levels.total_voice_minutes + EXCLUDED.total_voice_minutes,
        updated_at = now()
"""

DEDUCT_XP_SQL = """
    UPDATE user_levels
    SET xp = GREATEST(0, xp - $1),
        level = $2,
        updated_at = now()
    WHERE guild_id = $3 AND user_id = $4 AND faction = $5
"""

FETCH_USER_SQL = """
    SELECT xp, level
    FROM user_levels
    WHERE guild_id = $1 AND user_id = $2 AND faction = $3
"""

FETCH_ROLE_REWARD_SQL = """
    SELECT level, role_id
    FROM role_rewards
    WHERE guild_id = $1 AND faction = $2 AND level > $3 AND level <= $4
    ORDER BY level DESC
    LIMIT 1
"""

FETCH_CURRENT_REWARD_SQL = """
    SELECT current_reward_role
    FROM user_levels
    WHERE guild_id = $1 AND user_id = $2 AND faction = $3
"""

UPDATE_REWARD_ROLE_SQL = """
    UPDATE user_levels
    SET current_reward_role = $1, updated_at = now()
    WHERE guild_id = $2 AND user_id = $3 AND faction = $4
"""

GRANT_ARTEMISIUM_SQL = """
    UPDATE user_factions
    SET artemisium = artemisium + $1, updated_at = now()
    WHERE guild_id = $2 AND user_id = $3
"""


def xp_for_level(level: int, base: int, exponent: float) -> int:
    if level <= 0:
        return 0
    return int(base * math.pow(level, exponent) + base * level)


def total_xp_for_level(level: int, base: int, exponent: float) -> int:
    total = 0
    for i in range(1, level + 1):
        total += xp_for_level(i, base, exponent)
    return total


def level_from_xp(xp: int, base: int, exponent: float) -> int:
    level = 0
    accumulated = 0
    while True:
        needed = xp_for_level(level + 1, base, exponent)
        if accumulated + needed > xp:
            break
        accumulated += needed
        level += 1
    return level


@dataclass(slots=True)
class BufferEntry:
    faction: str
    delta_xp: int = 0
    delta_messages: int = 0
    delta_reactions: int = 0
    delta_voice_min: int = 0
    total_xp: int = 0
    level: int = 0
    needs_hydration: bool = True


@dataclass(slots=True)
class VoiceTracker:
    channel_id: str
    faction: str
    joined_at: float
    is_muted: bool = False
    muted_at: float = 0.0
    pending_xp: int = 0


class LevelingCog(commands.Cog, name="Leveling"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._xp_buffer: dict[tuple[str, str], BufferEntry] = {}
        self._buffer_lock = asyncio.Lock()
        self._voice_state: dict[tuple[str, str], VoiceTracker] = {}
        self._faction_role_map: dict[str, str] = {}
        self._blocked_channels: set[str] = set()
        self._blocked_roles: set[str] = set()
        self._settings: dict[str, Any] = {}

    async def cog_load(self) -> None:
        await self._sync_settings()
        await self._sync_faction_roles()
        await self._sync_exclusions()
        self._flush_loop.start()
        self._voice_xp_loop.start()
        logger.info(
            "Leveling online | factions=%d | blocked_ch=%d | blocked_r=%d",
            len(self._faction_role_map),
            len(self._blocked_channels),
            len(self._blocked_roles),
        )

    async def cog_unload(self) -> None:
        self._flush_loop.cancel()
        self._voice_xp_loop.cancel()
        await self.emergency_flush()

    async def emergency_flush(self) -> None:
        logger.warning("Emergency flush — %d buffer entries pending", len(self._xp_buffer))
        await self._flush_voice_pending()
        await self._flush_buffer()

    # ── Settings Hydration ────────────────────────────────────────────

    async def _sync_settings(self) -> None:
        gs = {}
        if hasattr(self.bot, "config_cache"):
            gs = self.bot.config_cache.get("guild_settings", {})
        self._settings = {
            "level_base": gs.get("level_base", DEFAULT_LEVEL_BASE),
            "level_exponent": gs.get("level_exponent", DEFAULT_LEVEL_EXPONENT),
            "msg_xp_min": gs.get("msg_xp_min", DEFAULT_MSG_XP_MIN),
            "msg_xp_max": gs.get("msg_xp_max", DEFAULT_MSG_XP_MAX),
            "msg_cooldown": gs.get("msg_cooldown_sec", DEFAULT_MSG_COOLDOWN),
            "react_xp": gs.get("react_xp", DEFAULT_REACT_XP),
            "react_cooldown": gs.get("react_cooldown_sec", DEFAULT_REACT_COOLDOWN),
            "voice_xp_per_min": gs.get("voice_xp_per_min", DEFAULT_VOICE_XP_PER_MIN),
            "announce_channel": gs.get("announce_channel_id"),
            "log_channel": gs.get("log_channel_id"),
        }

    async def _sync_faction_roles(self) -> None:
        guild_id = getattr(self.bot, "guild_id", None)
        if not guild_id:
            return
        rows = await database.fetch(
            "SELECT faction, base_role_id FROM faction_config WHERE guild_id = $1",
            guild_id,
        )
        self._faction_role_map = {row["base_role_id"]: row["faction"] for row in rows}

    async def _sync_exclusions(self) -> None:
        guild_id = getattr(self.bot, "guild_id", None)
        if not guild_id:
            return
        ch = await database.fetch(
            "SELECT channel_id FROM blocked_channels WHERE guild_id = $1", guild_id
        )
        self._blocked_channels = {r["channel_id"] for r in ch}
        rl = await database.fetch(
            "SELECT role_id FROM blocked_roles WHERE guild_id = $1", guild_id
        )
        self._blocked_roles = {r["role_id"] for r in rl}

    # ── Detection Helpers ─────────────────────────────────────────────

    def _detect_faction(self, member: discord.Member) -> Optional[str]:
        for role in member.roles:
            faction = self._faction_role_map.get(str(role.id))
            if faction:
                return faction
        return None

    def _is_blocked(self, channel_id: str, member: discord.Member) -> bool:
        if channel_id in self._blocked_channels:
            return True
        for role in member.roles:
            if str(role.id) in self._blocked_roles:
                return True
        return False

    # ── Cooldown Gate (Redis) ─────────────────────────────────────────

    async def _check_cooldown(self, ns: str, guild_id: str, user_id: str, ttl: int) -> bool:
        key = f"cd:{ns}:{guild_id}:{user_id}"
        if await redis_ipc.cache_exists(key):
            return True
        await redis_ipc.cache_set(key, "1", ttl_seconds=ttl)
        return False

    # ── Buffer Management ─────────────────────────────────────────────

    async def _hydrate_entry(self, guild_id: str, user_id: str, faction: str, entry: BufferEntry) -> None:
        row = await database.fetchrow(FETCH_USER_SQL, guild_id, user_id, faction)
        if row:
            entry.total_xp = row["xp"]
            entry.level = row["level"]
        else:
            entry.total_xp = 0
            entry.level = 0
        entry.needs_hydration = False

    async def _get_entry(self, guild_id: str, user_id: str, faction: str) -> BufferEntry:
        key = (guild_id, user_id)
        entry = self._xp_buffer.get(key)

        if entry and entry.faction == faction:
            if entry.needs_hydration:
                await self._hydrate_entry(guild_id, user_id, faction, entry)
            return entry

        entry = BufferEntry(faction=faction)
        await self._hydrate_entry(guild_id, user_id, faction, entry)
        self._xp_buffer[key] = entry
        return entry

    async def _grant_xp(
        self,
        guild_id: str,
        member: discord.Member,
        faction: str,
        xp_amount: int,
        source: str,
    ) -> None:
        user_id = str(member.id)
        key = (guild_id, user_id)

        # ── Phase 1: Read/create buffer entry ────────────────────────────────
        # No I/O inside the lock. Only memory operations.
        # If the entry needs DB hydration, we record that intent and exit the
        # lock before doing the network round-trip.
        async with self._buffer_lock:
            entry = self._xp_buffer.get(key)
            if entry is None or entry.faction != faction:
                entry = BufferEntry(faction=faction)
                self._xp_buffer[key] = entry
            needs_hydration = entry.needs_hydration

        if needs_hydration:
            await self._hydrate_entry(guild_id, user_id, faction, entry)

        async with self._buffer_lock:
            old_level = entry.level

            entry.delta_xp += xp_amount
            entry.total_xp += xp_amount

            if source == "message":
                entry.delta_messages += 1
            elif source == "reaction":
                entry.delta_reactions += 1
            elif source == "voice":
                xp_rate = max(1, self._settings.get("voice_xp_per_min", DEFAULT_VOICE_XP_PER_MIN))
                entry.delta_voice_min += max(1, xp_amount // xp_rate)

            new_level = level_from_xp(
                entry.total_xp,
                self._settings["level_base"],
                self._settings["level_exponent"],
            )
            entry.level = new_level
            buffer_size = len(self._xp_buffer)

        if new_level > old_level:
            asyncio.create_task(
                self._process_level_up(guild_id, member, faction, old_level, new_level)
            )

        if buffer_size >= BUFFER_COUNT_THRESHOLD:
            logger.info(
                "Buffer threshold hit (%d entries). Triggering early flush.", buffer_size
            )
            asyncio.create_task(self._flush_buffer())

    async def _process_level_up(
        self,
        guild_id: str,
        member: discord.Member,
        faction: str,
        old_level: int,
        new_level: int,
    ) -> None:
        try:
            logger.info(
                "Level up: %s [%s] %d → %d (faction=%s)",
                member, member.id, old_level, new_level, faction,
            )

            reward_row = await database.fetchrow(
                FETCH_ROLE_REWARD_SQL, guild_id, faction, old_level, new_level
            )
            if reward_row:
                await self._swap_reward_role(
                    guild_id, member, faction, reward_row["role_id"]
                )

            if old_level < ARTEMISIUM_LEVEL_GATE <= new_level:
                await database.execute(
                    GRANT_ARTEMISIUM_SQL, ARTEMISIUM_GRANT, guild_id, str(member.id)
                )
                logger.info(
                    "Artemisium +%d granted to %s (crossed level %d gate)",
                    ARTEMISIUM_GRANT, member.id, ARTEMISIUM_LEVEL_GATE,
                )

            announce_id = self._settings.get("announce_channel")
            if announce_id:
                ch = member.guild.get_channel(int(announce_id))
                if ch and isinstance(ch, discord.TextChannel):
                    label = "Member" if faction == "M" else "Friends"
                    suffix = ""
                    if reward_row:
                        suffix = f" They earned <@&{reward_row['role_id']}>!"
                    await ch.send(
                        f"\u2728 <@{member.id}> reached **Level {new_level}** "
                        f"on the **{label}** path!{suffix}"
                    )

            self.bot.dispatch("user_leveled_up", member, faction, old_level, new_level)

        except Exception:
            logger.exception("Level-up processing failed for %s", member.id)

    async def _swap_reward_role(
        self, guild_id: str, member: discord.Member, faction: str, new_role_id: str
    ) -> None:
        try:
            old_row = await database.fetchrow(
                FETCH_CURRENT_REWARD_SQL, guild_id, str(member.id), faction
            )
            old_role_id = old_row["current_reward_role"] if old_row and old_row["current_reward_role"] else None

            if old_role_id and old_role_id != new_role_id:
                old_role = member.guild.get_role(int(old_role_id))
                if old_role and old_role in member.roles:
                    await member.remove_roles(old_role, reason="Level reward superseded")

            new_role = member.guild.get_role(int(new_role_id))
            if new_role and new_role not in member.roles:
                await member.add_roles(new_role, reason="Level reward granted")

            await database.execute(
                UPDATE_REWARD_ROLE_SQL, new_role_id, guild_id, str(member.id), faction
            )
        except discord.Forbidden:
            logger.warning("Insufficient perms for role swap on %s", member.id)
        except Exception:
            logger.exception("Role swap failed for %s", member.id)

    # ── Periodic Flush ────────────────────────────────────────────────

    @tasks.loop(minutes=FLUSH_INTERVAL_MINUTES)
    async def _flush_loop(self) -> None:
        await self._flush_buffer()

    @_flush_loop.before_loop
    async def _before_flush(self) -> None:
        await self.bot.wait_until_ready()

    async def _flush_buffer(self) -> None:
        async with self._buffer_lock:
            if not self._xp_buffer:
                return
            snapshot = self._xp_buffer.copy()
            self._xp_buffer.clear()

        records: list[tuple] = []
        total_xp_map: dict[tuple[str, str], int] = {}

        for (guild_id, user_id), e in snapshot.items():
            if (
                e.delta_xp == 0
                and e.delta_messages == 0
                and e.delta_reactions == 0
                and e.delta_voice_min == 0
            ):
                continue
            records.append((
                guild_id, user_id, e.faction,
                e.delta_xp, e.level,
                e.delta_messages, e.delta_reactions, e.delta_voice_min,
            ))
            total_xp_map[(guild_id, user_id)] = e.total_xp

        if not records:
            return

        try:
            async with database.acquire() as conn:
                await conn.executemany(BATCH_UPSERT_SQL, records)
            logger.info("Flushed %d records to CockroachDB", len(records))
        except Exception:
            logger.exception("Flush FAILED — restoring %d entries to buffer", len(records))
            async with self._buffer_lock:
                for gid, uid, fac, dxp, lvl, dm, dr, dv in records:
                    key = (gid, uid)
                    existing = self._xp_buffer.get(key)
                    if existing:
                        existing.delta_xp += dxp
                        existing.delta_messages += dm
                        existing.delta_reactions += dr
                        existing.delta_voice_min += dv
                        existing.level = max(existing.level, lvl)
                    else:
                        self._xp_buffer[key] = BufferEntry(
                            faction=fac,
                            delta_xp=dxp,
                            delta_messages=dm,
                            delta_reactions=dr,
                            delta_voice_min=dv,
                            level=lvl,
                            total_xp=total_xp_map.get(key, dxp),
                            needs_hydration=False,
                        )

    @tasks.loop(minutes=1.0)
    async def _voice_xp_loop(self) -> None:
        await self._flush_voice_pending()

    @_voice_xp_loop.before_loop
    async def _before_voice(self) -> None:
        await self.bot.wait_until_ready()

    async def _flush_voice_pending(self) -> None:
        if not self._voice_state:
            return

        now = time.monotonic()
        xp_rate = self._settings.get("voice_xp_per_min", DEFAULT_VOICE_XP_PER_MIN)

        for (guild_id, user_id), t in list(self._voice_state.items()):
            elapsed = (now - t.joined_at) / 60.0
            if elapsed < 1.0:
                continue

            if t.is_muted and t.muted_at > 0:
                muted_min = (now - t.muted_at) / 60.0
                unmuted_min = max(0.0, elapsed - muted_min)
                gross = int(unmuted_min * xp_rate + muted_min * xp_rate * VOICE_MUTED_PENALTY)
            else:
                gross = int(elapsed * xp_rate)

            grant = gross - t.pending_xp
            if grant <= 0:
                continue
            t.pending_xp = gross

            guild = self.bot.get_guild(int(guild_id))
            member = guild.get_member(int(user_id)) if guild else None
            if not member:
                continue

            await self._grant_xp(guild_id, member, t.faction, grant, "voice")

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        if member.bot:
            return

        guild_id = str(member.guild.id)
        key = (guild_id, str(member.id))

        joined = before.channel is None and after.channel is not None
        left = before.channel is not None and after.channel is None
        switched = (
            before.channel is not None
            and after.channel is not None
            and before.channel.id != after.channel.id
        )

        if joined or (switched and key not in self._voice_state):
            faction = self._detect_faction(member)
            if faction and not self._is_blocked(str(after.channel.id), member):
                self._voice_state[key] = VoiceTracker(
                    channel_id=str(after.channel.id),
                    faction=faction,
                    joined_at=time.monotonic(),
                    is_muted=after.self_mute,
                    muted_at=time.monotonic() if after.self_mute else 0.0,
                )
            return

        if left:
            tracker = self._voice_state.pop(key, None)
            if tracker:
                await self._settle_voice(guild_id, member, tracker)
            return

        if switched:
            old = self._voice_state.pop(key, None)
            if old:
                await self._settle_voice(guild_id, member, old)
            faction = self._detect_faction(member)
            if faction and not self._is_blocked(str(after.channel.id), member):
                self._voice_state[key] = VoiceTracker(
                    channel_id=str(after.channel.id),
                    faction=faction,
                    joined_at=time.monotonic(),
                    is_muted=after.self_mute,
                    muted_at=time.monotonic() if after.self_mute else 0.0,
                )
            return

        if before.self_mute != after.self_mute and key in self._voice_state:
            t = self._voice_state[key]
            t.is_muted = after.self_mute
            t.muted_at = time.monotonic() if after.self_mute else 0.0

    async def _settle_voice(
        self, guild_id: str, member: discord.Member, tracker: VoiceTracker
    ) -> None:
        now = time.monotonic()
        elapsed = (now - tracker.joined_at) / 60.0
        if elapsed < 0.5:
            return

        xp_rate = self._settings.get("voice_xp_per_min", DEFAULT_VOICE_XP_PER_MIN)

        if tracker.is_muted and tracker.muted_at > 0:
            muted_min = (now - tracker.muted_at) / 60.0
            unmuted_min = max(0.0, elapsed - muted_min)
            gross = int(unmuted_min * xp_rate + muted_min * xp_rate * VOICE_MUTED_PENALTY)
        else:
            gross = int(elapsed * xp_rate)

        remaining = gross - tracker.pending_xp
        if remaining > 0:
            await self._grant_xp(guild_id, member, tracker.faction, remaining, "voice")

    # ── Discord Event Listeners ───────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return

        guild_id = str(message.guild.id)
        member = message.author

        if self._is_blocked(str(message.channel.id), member):
            return

        faction = self._detect_faction(member)
        if not faction:
            return

        on_cd = await self._check_cooldown(
            "msg", guild_id, str(member.id),
            self._settings.get("msg_cooldown", DEFAULT_MSG_COOLDOWN),
        )
        if on_cd:
            return

        xp = random.randint(
            self._settings.get("msg_xp_min", DEFAULT_MSG_XP_MIN),
            self._settings.get("msg_xp_max", DEFAULT_MSG_XP_MAX),
        )
        await self._grant_xp(guild_id, member, faction, xp, "message")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        if not payload.guild_id or payload.member is None or payload.member.bot:
            return

        guild_id = str(payload.guild_id)
        member = payload.member

        if self._is_blocked(str(payload.channel_id), member):
            return

        faction = self._detect_faction(member)
        if not faction:
            return

        on_cd = await self._check_cooldown(
            "react", guild_id, str(member.id),
            self._settings.get("react_cooldown", DEFAULT_REACT_COOLDOWN),
        )
        if on_cd:
            return

        xp = self._settings.get("react_xp", DEFAULT_REACT_XP)
        await self._grant_xp(guild_id, member, faction, xp, "reaction")

    # ── Observer: Apostle Punishment Intercept ─────────────────────────

    @commands.Cog.listener("on_user_punished")
    async def _handle_punishment(
        self, member: discord.Member, penalty: int, reason: str
    ) -> None:
        guild_id = str(member.guild.id)
        user_id = str(member.id)
        faction = self._detect_faction(member)

        if not faction:
            return

        logger.info(
            "Punishment intercept: %s — deducting %d XP (reason: %s)",
            member, penalty, reason,
        )

        async with self._buffer_lock:
            key = (guild_id, user_id)
            entry = self._xp_buffer.get(key)
            if entry:
                entry.total_xp = max(0, entry.total_xp - penalty)
                entry.delta_xp -= penalty
                entry.level = level_from_xp(
                    entry.total_xp,
                    self._settings["level_base"],
                    self._settings["level_exponent"],
                )

        try:
            row = await database.fetchrow(FETCH_USER_SQL, guild_id, user_id, faction)
            if row:
                new_xp = max(0, row["xp"] - penalty)
                new_level = level_from_xp(
                    new_xp,
                    self._settings["level_base"],
                    self._settings["level_exponent"],
                )
                await database.execute(
                    DEDUCT_XP_SQL, penalty, new_level, guild_id, user_id, faction
                )
        except Exception:
            logger.exception("Punishment persistence failed for %s", user_id)

    # ── Hot-Reload Listeners ──────────────────────────────────────────

    @commands.Cog.listener("on_guild_settings_reloaded")
    async def _on_settings_reload(self, old: dict, new: dict) -> None:
        await self._sync_settings()
        logger.info("Leveling settings hot-reloaded")

    @commands.Cog.listener("on_config_reloaded")
    async def _on_config_reload(self, old: dict, new: dict) -> None:
        await self._sync_faction_roles()
        await self._sync_exclusions()
        logger.info("Faction roles and exclusions hot-reloaded")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(LevelingCog(bot))
