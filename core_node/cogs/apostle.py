import asyncio
import re
import json
import time
import logging
import datetime
from typing import Any, Optional

import aiohttp
import discord
from discord.ext import commands

from shared_lib import database

logger = logging.getLogger("twomoon.cog.apostle")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

PUNISHMENT_XP_PENALTY = {
    "WARN": 100,
    "MUTE": 500,
    "KICK": 1000,
    "BAN": 0,
}

PUNISHMENT_TIMEOUT_DURATION = {
    "WARN": None,
    "MUTE": datetime.timedelta(hours=1),
    "KICK": None,
    "BAN": None,
}

DEFAULT_SYSTEM_PROMPT = (
    "You are a Discord server moderation AI. Analyze the following user message "
    "for toxicity, hate speech, harassment, spam, or rule violations.\n\n"
    "Respond ONLY with a JSON object in this exact format:\n"
    '{"toxic": true/false, "reason": "brief explanation", '
    '"confidence": 0.0-1.0, "tier": "WARN/MUTE/KICK/BAN"}\n\n'
    "If the message is benign, respond: "
    '{"toxic": false, "reason": "clean", "confidence": 1.0, "tier": "NONE"}\n\n'
    "RULES IN EFFECT:\n"
)

AI_CONFIDENCE_THRESHOLD = 0.75


class CompiledRule:
    __slots__ = ("rule_id", "rule_name", "pattern", "compiled", "tier", "strike_duration")

    def __init__(self, row: dict):
        self.rule_id: str = str(row["rule_id"])
        self.rule_name: str = row["rule_name"]
        self.pattern: str = row["pattern"]
        self.tier: str = row["punishment_tier"]
        self.strike_duration: int = row.get("strike_duration_sec", 86400)
        try:
            self.compiled: Optional[re.Pattern] = re.compile(self.pattern, re.IGNORECASE)
        except re.error:
            self.compiled = None
            logger.error("Invalid regex in rule '%s': %s", self.rule_name, self.pattern)


class ApostleCog(commands.Cog, name="Apostle"):

    MAX_CONCURRENT_AI = 10
    AI_TIMEOUT_SECONDS = 15.0
    CIRCUIT_FAILURE_THRESHOLD = 5
    CIRCUIT_RECOVERY_SECONDS = 60.0

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_AI)
        self._compiled_rules: list[CompiledRule] = []
        self._consecutive_failures: int = 0
        self._last_failure_time: float = 0.0
        self._circuit_probe_active: bool = False
        self._total_analyzed: int = 0
        self._total_caught: int = 0

    async def cog_load(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=self.MAX_CONCURRENT_AI,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
            ),
            timeout=aiohttp.ClientTimeout(total=30.0),
        )
        self._recompile_rules()
        logger.info(
            "Apostle cog loaded | compiled_rules=%d | max_concurrent=%d",
            len(self._compiled_rules),
            self.MAX_CONCURRENT_AI,
        )

    async def cog_unload(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info(
            "Apostle cog unloaded | analyzed=%d | caught=%d",
            self._total_analyzed,
            self._total_caught,
        )

    def _recompile_rules(self) -> None:
        raw_rules = getattr(self.bot, "moderation_rules_cache", [])
        self._compiled_rules = [CompiledRule(r) for r in raw_rules]
        logger.info("Compiled %d moderation rules", len(self._compiled_rules))

    def _cfg(self, key: str, default: Any = None) -> Any:
        return getattr(self.bot, "config_cache", {}).get(key, default)

    def _is_ai_enabled(self) -> bool:
        flags = self._cfg("feature_flags", {})
        return flags.get("ai_moderation_enabled", True)

    def _get_api_key(self) -> Optional[str]:
        return self._cfg("openrouter_api_key")

    def _get_model_id(self) -> str:
        return self._cfg("ai_model_id", "deepseek/deepseek-chat")

    def _get_system_prompt(self) -> str:
        return self._cfg("ai_system_prompt", "")

    def _check_regex(self, content: str) -> Optional[CompiledRule]:
        for rule in self._compiled_rules:
            if rule.compiled and rule.compiled.search(content):
                return rule
        return None

    def _build_system_message(self) -> str:
        custom_prompt = self._get_system_prompt()
        if custom_prompt:
            base = custom_prompt
        else:
            base = DEFAULT_SYSTEM_PROMPT

        rule_lines = []
        for rule in self._compiled_rules:
            rule_lines.append(f"- {rule.rule_name} (tier: {rule.tier})")

        if rule_lines:
            return base + "\n".join(rule_lines)
        return base

    def _circuit_should_skip(self) -> bool:
        if self._consecutive_failures < self.CIRCUIT_FAILURE_THRESHOLD:
            return False

        elapsed = time.monotonic() - self._last_failure_time
        if elapsed < self.CIRCUIT_RECOVERY_SECONDS:
            return True

        if self._circuit_probe_active:
            return True

        self._circuit_probe_active = True
        logger.info(
            "Circuit breaker HALF-OPEN: %.1fs since last failure, allowing probe request",
            elapsed,
        )
        return False

    def _circuit_on_success(self) -> None:
        was_open = self._consecutive_failures >= self.CIRCUIT_FAILURE_THRESHOLD
        self._consecutive_failures = 0
        self._last_failure_time = 0.0
        self._circuit_probe_active = False
        if was_open:
            logger.info("Circuit breaker CLOSED: probe succeeded, AI moderation fully restored")

    def _circuit_on_failure(self) -> None:
        self._consecutive_failures += 1
        self._last_failure_time = time.monotonic()
        was_probe = self._circuit_probe_active
        self._circuit_probe_active = False
        if was_probe:
            logger.warning(
                "Circuit breaker remains OPEN: probe failed (consecutive=%d)",
                self._consecutive_failures,
            )
        elif self._consecutive_failures == self.CIRCUIT_FAILURE_THRESHOLD:
            logger.error(
                "Circuit breaker OPEN: %d consecutive AI failures, entering degraded mode",
                self._consecutive_failures,
            )

    async def _call_openrouter(self, content: str) -> Optional[dict]:
        api_key = self._get_api_key()
        if not api_key:
            return None

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://twomoon.ecosystem",
            "X-Title": "Two Moon Apostle",
        }

        payload = {
            "model": self._get_model_id(),
            "messages": [
                {"role": "system", "content": self._build_system_message()},
                {"role": "user", "content": content},
            ],
            "temperature": 0.1,
            "max_tokens": 150,
        }

        start = time.monotonic()

        try:
            raw_response = await asyncio.wait_for(
                self._fetch_completion(headers, payload),
                timeout=self.AI_TIMEOUT_SECONDS,
            )
            elapsed = (time.monotonic() - start) * 1000
            self._circuit_on_success()
            logger.debug("OpenRouter responded in %.0fms", elapsed)
            return raw_response

        except asyncio.TimeoutError:
            elapsed = (time.monotonic() - start) * 1000
            self._circuit_on_failure()
            logger.warning("OpenRouter timeout after %.0fms", elapsed)
            return None

        except aiohttp.ClientError as e:
            self._circuit_on_failure()
            logger.warning("OpenRouter network error: %s", type(e).__name__)
            return None

        except Exception as e:
            self._circuit_on_failure()
            logger.error("Unexpected OpenRouter error: %s", e, exc_info=True)
            return None

    async def _fetch_completion(self, headers: dict, payload: dict) -> dict:
        async with self._session.post(OPENROUTER_URL, headers=headers, json=payload) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _parse_verdict(self, api_response: dict) -> tuple[bool, str, float, str]:
        try:
            choices = api_response.get("choices", [])
            if not choices:
                return False, "", 0.0, "NONE"

            raw_text = choices[0].get("message", {}).get("content", "")
            cleaned = raw_text.strip()

            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            verdict = json.loads(cleaned)

            is_toxic = verdict.get("toxic", False)
            reason = verdict.get("reason", "No reason provided")
            confidence = float(verdict.get("confidence", 0.0))
            tier = verdict.get("tier", "WARN").upper()

            if tier not in PUNISHMENT_XP_PENALTY:
                tier = "WARN"

            return is_toxic, reason, confidence, tier

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning("Failed to parse AI verdict: %s | raw=%s", e, api_response)
            return False, "", 0.0, "NONE"

    async def _record_strike(
        self,
        guild_id: str,
        user_id: str,
        rule_id: Optional[str],
        action: str,
        reason: str,
        duration_sec: int = 86400,
    ) -> None:
        expires_at = None
        if duration_sec > 0:
            expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=duration_sec
            )

        try:
            await database.execute(
                "INSERT INTO moderation_strikes "
                "(guild_id, user_id, rule_id, moderator_type, action_taken, reason, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                guild_id,
                user_id,
                rule_id,
                "AI",
                action,
                reason,
                expires_at,
            )
        except Exception as e:
            logger.error("Failed to record strike for user %s: %s", user_id, e)

    async def _execute_punishment(
        self,
        message: discord.Message,
        tier: str,
        reason: str,
        rule_id: Optional[str] = None,
        strike_duration: int = 86400,
    ) -> None:
        guild_id = str(message.guild.id)
        user_id = str(message.author.id)
        member = message.author

        try:
            await message.delete()
        except discord.NotFound:
            logger.debug("Message %s already deleted, proceeding with strike", message.id)
        except (discord.Forbidden, discord.HTTPException) as e:
            logger.warning("Failed to delete message %s: %s", message.id, e)

        await self._record_strike(guild_id, user_id, rule_id, tier, reason, strike_duration)
        self._total_caught += 1

        timeout_delta = PUNISHMENT_TIMEOUT_DURATION.get(tier)
        if timeout_delta and isinstance(member, discord.Member):
            try:
                until = discord.utils.utcnow() + timeout_delta
                await member.timeout(until, reason=f"[Apostle] {reason}")
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.warning("Failed to timeout %s: %s", user_id, e)

        if tier == "KICK" and isinstance(member, discord.Member):
            try:
                await member.kick(reason=f"[Apostle] {reason}")
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.warning("Failed to kick %s: %s", user_id, e)

        if tier == "BAN" and isinstance(member, discord.Member):
            try:
                await message.guild.ban(member, reason=f"[Apostle] {reason}", delete_message_days=0)
            except (discord.Forbidden, discord.HTTPException) as e:
                logger.warning("Failed to ban %s: %s", user_id, e)

        penalty = PUNISHMENT_XP_PENALTY.get(tier, 0)
        if penalty > 0 and isinstance(member, discord.Member):
            self.bot.dispatch("user_punished", member, penalty, reason)
            logger.info(
                "Dispatched 'user_punished': user=%s tier=%s penalty=%d reason=%s",
                user_id, tier, penalty, reason,
            )

        await self._send_to_log_channel(message, tier, reason, rule_id)

        if tier in ("WARN", "MUTE"):
            try:
                await member.send(
                    f"\u26a0\ufe0f **Moderation Notice — Two Moon**\n"
                    f"Your message was flagged and removed.\n"
                    f"**Action:** {tier}\n"
                    f"**Reason:** {reason}"
                )
            except (discord.Forbidden, discord.HTTPException):
                pass

    async def _send_to_log_channel(
        self,
        message: discord.Message,
        tier: str,
        reason: str,
        rule_id: Optional[str],
    ) -> None:
        log_channel_id = self._cfg("log_channel_id")
        if not log_channel_id:
            return

        channel = message.guild.get_channel(int(log_channel_id))
        if not channel or not isinstance(channel, discord.TextChannel):
            return

        embed = discord.Embed(
            title=f"\U0001f6e1\ufe0f Apostle — {tier}",
            color=discord.Color.red(),
            timestamp=discord.utils.utcnow(),
        )
        embed.add_field(name="User", value=f"{message.author.mention} (`{message.author.id}`)", inline=True)
        embed.add_field(name="Channel", value=message.channel.mention, inline=True)
        embed.add_field(name="Tier", value=tier, inline=True)
        embed.add_field(name="Reason", value=reason[:1024], inline=False)

        content_preview = message.content[:500] if message.content else "(empty)"
        embed.add_field(name="Message Content", value=f"```{content_preview}```", inline=False)

        if rule_id:
            embed.set_footer(text=f"Rule ID: {rule_id}")
        else:
            embed.set_footer(text="Detection: AI Analysis")

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            pass

    async def _analyze_message(self, message: discord.Message) -> None:
        self._total_analyzed += 1
        content = message.content

        if not content or len(content) < 3:
            return

        matched_rule = self._check_regex(content)
        if matched_rule:
            logger.info(
                "Regex match: rule='%s' user=%s",
                matched_rule.rule_name,
                message.author.id,
            )
            await self._execute_punishment(
                message,
                tier=matched_rule.tier,
                reason=f"Matched rule: {matched_rule.rule_name}",
                rule_id=matched_rule.rule_id,
                strike_duration=matched_rule.strike_duration,
            )
            return

        if not self._is_ai_enabled():
            return

        if not self._get_api_key():
            return

        if self._circuit_should_skip():
            return

        if self._semaphore.locked():
            logger.debug("AI semaphore saturated, skipping AI for msg %s", message.id)
            return

        async with self._semaphore:
            api_response = await self._call_openrouter(content)

        if api_response is None:
            return

        is_toxic, reason, confidence, tier = self._parse_verdict(api_response)

        if not is_toxic or confidence < AI_CONFIDENCE_THRESHOLD:
            return

        logger.info(
            "AI verdict: toxic=%s confidence=%.2f tier=%s user=%s",
            is_toxic, confidence, tier, message.author.id,
        )

        await self._execute_punishment(
            message,
            tier=tier,
            reason=f"[AI] {reason}",
            rule_id=None,
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return

        if not message.content:
            return

        flags = self._cfg("feature_flags", {})
        if not flags.get("moderation_enabled", True):
            return

        guild_id = getattr(self.bot, "guild_id", None)
        if guild_id and str(message.guild.id) != guild_id:
            return

        self.bot.loop.create_task(
            self._analyze_message(message),
            name=f"apostle-{message.id}",
        )

    @commands.Cog.listener("on_config_reloaded")
    async def on_config_reloaded(self, old_config: dict, new_config: dict) -> None:
        logger.info("Config reloaded, Apostle refreshing AI parameters")

    @commands.Cog.listener("on_rules_reloaded")
    async def on_rules_reloaded(self, old_rules: list, new_rules: list) -> None:
        self._recompile_rules()
        logger.info("Moderation rules recompiled via hot-reload")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ApostleCog(bot))
