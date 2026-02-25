import asyncio
import datetime
import logging
import secrets
import string
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands

from shared_lib import database
from shared_lib import redis_ipc

logger = logging.getLogger("twomoon.cog.vouch")

VOUCH_COOLDOWN_PREFIX = "cd:vouch:"

VOUCH_AUTHORITY = {
    "OWNER": {"cooldown_sec": 0, "rep_value": 50},
    "MOD": {"cooldown_sec": 0, "rep_value": 20},
    "ALL_STARS": {"cooldown_sec": 600, "rep_value": 10},
    "KAISER": {"cooldown_sec": 3600, "rep_value": 5},
    "WARLORD": {"cooldown_sec": 3600, "rep_value": 5},
}


def _generate_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


class VouchCog(commands.Cog, name="Vouch"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._authority_role_map: dict[str, str] = {}
        self._candidate_role_id: Optional[str] = None
        self._member_base_role_id: Optional[str] = None

    async def cog_load(self) -> None:
        await self._hydrate_vouch_config()
        logger.info("Vouch cog loaded")

    async def _hydrate_vouch_config(self) -> None:
        guild_id = getattr(self.bot, "guild_id", None)
        if not guild_id:
            return

        config = getattr(self.bot, "config_cache", {})
        flags = config.get("feature_flags", {})
        vouch_roles = flags.get("vouch_authority_roles", {})
        self._authority_role_map = vouch_roles
        self._candidate_role_id = flags.get("candidate_role_id")

        row = await database.fetchrow(
            "SELECT base_role_id, promotion_rules FROM faction_config "
            "WHERE guild_id = $1 AND faction = 'M'",
            guild_id,
        )
        if row:
            self._member_base_role_id = row["base_role_id"]
            rules = row.get("promotion_rules") or {}
            if not self._candidate_role_id:
                self._candidate_role_id = rules.get("candidate_role_id")

    def _resolve_authority(self, member: discord.Member) -> Optional[str]:
        member_role_ids = {str(r.id) for r in member.roles}

        for tier in ("OWNER", "MOD", "ALL_STARS", "KAISER", "WARLORD"):
            role_id = self._authority_role_map.get(tier)
            if role_id and role_id in member_role_ids:
                return tier

        if member.guild_permissions.administrator:
            return "OWNER"

        return None

    @app_commands.command(name="vouch", description="Vouch for a user to join the Member path")
    @app_commands.describe(target="The user you want to vouch for")
    async def vouch_command(
        self,
        interaction: discord.Interaction,
        target: discord.Member,
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        guild_id = str(interaction.guild_id)
        invoker = interaction.user

        if not isinstance(invoker, discord.Member):
            await interaction.followup.send("This command can only be used in a server.", ephemeral=True)
            return

        if target.bot:
            await interaction.followup.send("You cannot vouch for a bot.", ephemeral=True)
            return

        if target.id == invoker.id:
            await interaction.followup.send("You cannot vouch for yourself.", ephemeral=True)
            return

        authority = self._resolve_authority(invoker)
        if not authority:
            await interaction.followup.send(
                "You do not have the authority to vouch. "
                "Only established Members with specific ranks can vouch.",
                ephemeral=True,
            )
            return

        tier = VOUCH_AUTHORITY[authority]
        cooldown_sec = tier["cooldown_sec"]
        rep_value = tier["rep_value"]

        if cooldown_sec > 0:
            cooldown_key = f"{VOUCH_COOLDOWN_PREFIX}{guild_id}:{invoker.id}"
            is_cooling = await redis_ipc.cache_exists(cooldown_key)
            if is_cooling:
                await interaction.followup.send(
                    f"You're on cooldown. Please wait before vouching again.",
                    ephemeral=True,
                )
                return

        existing_faction = await database.fetchrow(
            "SELECT faction, faction_status FROM user_factions "
            "WHERE guild_id = $1 AND user_id = $2",
            guild_id,
            str(target.id),
        )

        if existing_faction:
            faction = existing_faction["faction"]
            status = existing_faction["faction_status"]
            if faction == "M" and status in ("CANDIDATE", "MEMBER"):
                await interaction.followup.send(
                    f"{target.mention} is already a Candidate or Member.",
                    ephemeral=True,
                )
                return
            if faction == "F":
                await interaction.followup.send(
                    f"{target.mention} is on the Friends path. "
                    "They must leave it first before being vouched into Member.",
                    ephemeral=True,
                )
                return

        already_vouched = await database.fetchrow(
            "SELECT code FROM vouch_codes "
            "WHERE guild_id = $1 AND used_by = $2 AND status = 'USED'",
            guild_id,
            str(target.id),
        )
        if already_vouched:
            await interaction.followup.send(
                f"{target.mention} has already been vouched in.",
                ephemeral=True,
            )
            return

        code = _generate_code()

        try:
            async with database.transaction() as conn:
                await conn.execute(
                    "INSERT INTO vouch_codes "
                    "(code, guild_id, creator_id, rep_value, status, used_by, used_at, expires_at) "
                    "VALUES ($1, $2, $3, $4, 'USED', $5, now(), now() + interval '3 days')",
                    code,
                    guild_id,
                    str(invoker.id),
                    rep_value,
                    str(target.id),
                )

                await conn.execute(
                    "INSERT INTO user_factions (guild_id, user_id, faction, faction_status) "
                    "VALUES ($1, $2, 'M', 'CANDIDATE') "
                    "ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                    "faction = 'M', faction_status = 'CANDIDATE', updated_at = now()",
                    guild_id,
                    str(target.id),
                )

                await conn.execute(
                    "INSERT INTO user_profiles (guild_id, user_id, reputation, voucher_id, first_redeemed_at) "
                    "VALUES ($1, $2, $3, $4, now()) "
                    "ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                    "reputation = user_profiles.reputation + $3, "
                    "voucher_id = $4, updated_at = now()",
                    guild_id,
                    str(target.id),
                    rep_value,
                    str(invoker.id),
                )

        except Exception as e:
            logger.error("Vouch DB transaction failed: %s", e)
            await interaction.followup.send(
                "An error occurred while processing the vouch. Please try again.",
                ephemeral=True,
            )
            return

        if self._candidate_role_id:
            candidate_role = interaction.guild.get_role(int(self._candidate_role_id))
            if candidate_role and candidate_role not in target.roles:
                try:
                    await target.add_roles(
                        candidate_role,
                        reason=f"Vouched by {invoker} ({authority})",
                    )
                except discord.HTTPException as e:
                    logger.warning("Failed to assign candidate role: %s", e)

        if cooldown_sec > 0:
            cooldown_key = f"{VOUCH_COOLDOWN_PREFIX}{guild_id}:{invoker.id}"
            await redis_ipc.cache_set(cooldown_key, "1", ttl_seconds=cooldown_sec)

        embed = discord.Embed(
            title="\U0001f91d Vouch Confirmed",
            description=(
                f"{target.mention} has been vouched into the **Member Path** "
                f"by {invoker.mention}."
            ),
            color=discord.Color.from_rgb(99, 102, 241),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="Status", value="Candidate", inline=True)
        embed.add_field(name="Reputation Granted", value=f"+{rep_value}", inline=True)
        embed.add_field(name="Voucher Authority", value=authority, inline=True)
        embed.set_footer(text=f"Code: {code}")

        await interaction.followup.send(embed=embed)

        logger.info(
            "Vouch completed: invoker=%s target=%s authority=%s rep=%d code=%s",
            invoker.id, target.id, authority, rep_value, code,
        )

        self.bot.dispatch("user_vouched", invoker, target, authority, rep_value)

    @commands.Cog.listener("on_config_reloaded")
    async def on_config_reloaded(self, old_config: dict, new_config: dict) -> None:
        await self._hydrate_vouch_config()
        logger.info("Vouch config rehydrated via hot-reload")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(VouchCog(bot))
