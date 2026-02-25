import datetime
import logging
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from shared_lib import database

logger = logging.getLogger("twomoon.cog.faction")


class FactionCog(commands.Cog, name="Faction"):

    FACTION_MAP = {"Member": "M", "Friends": "F"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._faction_configs: dict[str, dict[str, Any]] = {}
        self._friend_promotion_days: int = 7
        self._active_today: set[str] = set()
        self._active_today_date: datetime.date = datetime.date.today()

    async def cog_load(self) -> None:
        await self._hydrate_faction_configs()
        self._promotion_sweep.start()
        self._date_rollover_check.start()
        logger.info("Faction cog loaded")

    async def cog_unload(self) -> None:
        self._promotion_sweep.cancel()
        self._date_rollover_check.cancel()

    async def _hydrate_faction_configs(self) -> None:
        guild_id = getattr(self.bot, "guild_id", None)
        if not guild_id:
            return

        rows = await database.fetch(
            "SELECT faction, base_role_id, promotion_rules "
            "FROM faction_config WHERE guild_id = $1",
            guild_id,
        )
        self._faction_configs = {r["faction"]: dict(r) for r in rows}

        f_config = self._faction_configs.get("F", {})
        rules = f_config.get("promotion_rules") or {}
        self._friend_promotion_days = rules.get("friend_promotion_days", 7)

        logger.info(
            "Faction configs hydrated: %s | friend_promo_days=%d",
            list(self._faction_configs.keys()),
            self._friend_promotion_days,
        )

    def _get_base_role_id(self, faction: str) -> Optional[str]:
        cfg = self._faction_configs.get(faction)
        return cfg.get("base_role_id") if cfg else None

    def _get_all_faction_role_ids(self) -> list[str]:
        ids = []
        for cfg in self._faction_configs.values():
            rid = cfg.get("base_role_id")
            if rid:
                ids.append(rid)
        return ids

    def _maybe_rollover(self) -> None:
        today = datetime.date.today()
        if today != self._active_today_date:
            old_size = len(self._active_today)
            self._active_today.clear()
            self._active_today_date = today
            if old_size > 0:
                logger.info("Active-today set rolled over: cleared %d entries for %s", old_size, today)

    @app_commands.command(name="path", description="Choose your faction path: Member or Friends")
    @app_commands.describe(choice="The path you want to join")
    @app_commands.choices(
        choice=[
            app_commands.Choice(name="Member", value="Member"),
            app_commands.Choice(name="Friends", value="Friends"),
        ]
    )
    async def path_command(
        self,
        interaction: discord.Interaction,
        choice: app_commands.Choice[str],
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        guild_id = str(interaction.guild_id)
        user_id = str(interaction.user.id)
        member = interaction.user

        if not isinstance(member, discord.Member):
            await interaction.followup.send(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            return

        faction_key = self.FACTION_MAP.get(choice.value)
        if not faction_key:
            await interaction.followup.send("Invalid path choice.", ephemeral=True)
            return

        new_base_role_id = self._get_base_role_id(faction_key)
        if not new_base_role_id:
            await interaction.followup.send(
                f"The **{choice.value}** path is not configured for this server.",
                ephemeral=True,
            )
            return

        existing = await database.fetchrow(
            "SELECT faction, faction_status FROM user_factions "
            "WHERE guild_id = $1 AND user_id = $2",
            guild_id,
            user_id,
        )

        if existing:
            current_faction = existing["faction"]
            current_status = existing["faction_status"]

            if current_faction == faction_key:
                await interaction.followup.send(
                    f"You are already on the **{choice.value}** path "
                    f"(status: {current_status}).",
                    ephemeral=True,
                )
                return

            if current_faction == "M" and current_status in ("CANDIDATE", "MEMBER"):
                await interaction.followup.send(
                    "You have been vouched into the **Member** path and cannot switch. "
                    "Contact a moderator if you need to change factions.",
                    ephemeral=True,
                )
                return

        if faction_key == "M":
            has_vouch = await database.fetchrow(
                "SELECT code FROM vouch_codes "
                "WHERE guild_id = $1 AND used_by = $2 AND status = 'USED'",
                guild_id,
                user_id,
            )
            if not has_vouch:
                await interaction.followup.send(
                    "The **Member** path requires a vouch from an existing member. "
                    "Ask someone to use `/vouch` on you, or choose the **Friends** path.",
                    ephemeral=True,
                )
                return
            initial_status = "CANDIDATE"
        else:
            initial_status = "VISITOR"

        today = datetime.date.today()

        try:
            await database.execute(
                "INSERT INTO user_factions "
                "(guild_id, user_id, faction, faction_status, total_active_days, last_active_date) "
                "VALUES ($1, $2, $3, $4, 0, $5) "
                "ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                "faction = $3, faction_status = $4, total_active_days = 0, "
                "last_active_date = $5, updated_at = now()",
                guild_id,
                user_id,
                faction_key,
                initial_status,
                today,
            )
        except Exception as e:
            logger.error("Failed to upsert faction for %s: %s", user_id, e)
            await interaction.followup.send(
                "An error occurred. Please try again.",
                ephemeral=True,
            )
            return

        roles_to_remove = []
        all_faction_ids = self._get_all_faction_role_ids()
        for role in member.roles:
            if str(role.id) in all_faction_ids and str(role.id) != new_base_role_id:
                roles_to_remove.append(role)

        new_role = interaction.guild.get_role(int(new_base_role_id))

        try:
            if roles_to_remove:
                await member.remove_roles(
                    *roles_to_remove,
                    reason=f"Faction switch to {choice.value}",
                )

            if new_role and new_role not in member.roles:
                await member.add_roles(
                    new_role,
                    reason=f"Joined {choice.value} path via /path",
                )
        except discord.HTTPException as e:
            logger.warning("Role modification failed for %s: %s", user_id, e)

        faction_label = choice.value
        path_color = (
            discord.Color.from_rgb(99, 102, 241)
            if faction_key == "M"
            else discord.Color.from_rgb(34, 197, 94)
        )

        embed = discord.Embed(
            title=f"\U0001f319 Path Chosen: {faction_label}",
            description=(
                f"{member.mention} has joined the **{faction_label}** path!\n\n"
                + (
                    "Welcome, Candidate. Prove yourself to rise through the ranks."
                    if faction_key == "M"
                    else "Welcome, Visitor. Be active for 7 days to earn Friends status."
                )
            ),
            color=path_color,
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Faction", value=faction_label, inline=True)
        embed.add_field(name="Status", value=initial_status, inline=True)
        embed.set_footer(text="Two Moon — Two Paths")

        await interaction.followup.send(embed=embed)

        logger.info(
            "Path chosen: user=%s faction=%s status=%s",
            user_id, faction_key, initial_status,
        )

        self.bot.dispatch("faction_chosen", member, faction_key, initial_status)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return

        guild_id = str(message.guild.id)
        user_id = str(message.author.id)
        cache_key = f"{guild_id}:{user_id}"

        self._maybe_rollover()

        if cache_key in self._active_today:
            return

        today = datetime.date.today()

        try:
            result = await database.fetchval(
                "UPDATE user_factions SET "
                "total_active_days = total_active_days + 1, "
                "last_active_date = $1, "
                "updated_at = now() "
                "WHERE guild_id = $2 AND user_id = $3 "
                "AND (last_active_date IS NULL OR last_active_date < $1) "
                "RETURNING total_active_days",
                today,
                guild_id,
                user_id,
            )
        except Exception:
            return

        self._active_today.add(cache_key)

        if result is not None:
            logger.debug(
                "Active day recorded: user=%s total=%d",
                user_id, result,
            )

    @tasks.loop(hours=1)
    async def _promotion_sweep(self) -> None:
        guild_id = getattr(self.bot, "guild_id", None)
        if not guild_id:
            return

        guild = self.bot.get_guild(int(guild_id))
        if not guild:
            return

        rows = await database.fetch(
            "SELECT user_id, total_active_days "
            "FROM user_factions "
            "WHERE guild_id = $1 AND faction = 'F' AND faction_status = 'VISITOR' "
            "AND total_active_days >= $2",
            guild_id,
            self._friend_promotion_days,
        )

        if not rows:
            return

        promotion_batch = [(guild_id, r["user_id"]) for r in rows]
        promotion_members: list[tuple[str, Optional[discord.Member]]] = [
            (r["user_id"], guild.get_member(int(r["user_id"]))) for r in rows
        ]

        try:
            async with database.acquire() as conn:
                await conn.executemany(
                    "UPDATE user_factions SET "
                    "faction_status = 'FRIEND', updated_at = now() "
                    "WHERE guild_id = $1 AND user_id = $2",
                    promotion_batch,
                )
        except Exception as e:
            logger.error("Batch promotion update failed: %s", e)
            return

        f_config = self._faction_configs.get("F", {})
        rules = f_config.get("promotion_rules") or {}
        friend_role_id = rules.get("friend_role_id")
        friend_role = None
        if friend_role_id:
            friend_role = guild.get_role(int(friend_role_id))

        granted = 0
        for user_id, member in promotion_members:
            if not member:
                continue
            try:
                if friend_role and friend_role not in member.roles:
                    await member.add_roles(
                        friend_role,
                        reason=f"Promoted to Friends ({self._friend_promotion_days} active days)",
                    )
                    granted += 1
                self.bot.dispatch("faction_promoted", member, "F", "FRIEND")
            except discord.HTTPException as e:
                logger.warning("Promotion role grant failed for %s: %s", user_id, e)

        logger.info(
            "Promotion sweep: %d promoted, %d roles granted",
            len(promotion_batch),
            granted,
        )

    @_promotion_sweep.before_loop
    async def _before_sweep(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=5)
    async def _date_rollover_check(self) -> None:
        self._maybe_rollover()

    @_date_rollover_check.before_loop
    async def _before_rollover(self) -> None:
        await self.bot.wait_until_ready()

    @commands.Cog.listener("on_guild_settings_reloaded")
    async def on_guild_settings_reloaded(self, old_config: dict, new_config: dict) -> None:
        await self._hydrate_faction_configs()
        logger.info("Faction configs rehydrated via hot-reload")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(FactionCog(bot))
