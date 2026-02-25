"""
web_hub/services/audit.py
─────────────────────────
Shared audit log writer for the Two Moon Web Hub.

Centralises the _record_audit helper that was previously duplicated in:
  - web_hub/main.py
  - web_hub/api/config.py

Usage:
    from web_hub.services.audit import record_audit
    await record_audit(user, "CONFIG_UPDATE", "bot_config", bot_id)
"""

import json
import logging
from typing import Any, Optional

from shared_lib import database

logger = logging.getLogger("twomoon.web_hub.audit")

_MAX_DETAILS_LEN = 2000


async def record_audit(
    user: dict[str, Any],
    action: str,
    target_type: str,
    target_id: str,
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """
    Write a row to ``audit_log``.

    Parameters
    ----------
    user:
        The decoded JWT payload from the authenticated request.
        Must contain at minimum ``"sub"`` (actor Discord ID) and
        ``"clearance"`` / ``"username"`` for the details JSON.
    action:
        Free-form action label, e.g. ``"CONFIG_UPDATE"``.
    target_type:
        Entity type being acted on, e.g. ``"bot_config"``, ``"guild_settings"``.
    target_id:
        Primary identifier of the target entity (UUID string, guild_id, etc.).
    extra:
        Optional dict of additional metadata merged into the ``details`` JSON
        column. Serialisation errors are handled gracefully.

    Notes
    -----
    Failures are logged at ERROR level but never propagated — a failing audit
    write must never abort the main operation that triggered it.

    ``audit_log.guild_id`` is nullable (schema v1.1) so this helper does not
    attempt to resolve or supply it; callers that have the guild_id available
    should pass it via ``extra={"guild_id": guild_id}`` to aid future queries.
    """
    details: dict[str, Any] = {
        "clearance": user.get("clearance", "unknown"),
        "username": user.get("username", "unknown"),
    }
    if extra:
        details.update(extra)

    try:
        details_json = json.dumps(details, ensure_ascii=False)
    except (TypeError, ValueError):
        # Fallback: stringify every value so we always emit valid JSON
        details_json = json.dumps(
            {k: str(v) for k, v in details.items()},
            ensure_ascii=False,
        )

    # Hard-cap to avoid filling the JSONB column with enormous payloads
    if len(details_json) > _MAX_DETAILS_LEN:
        details_json = details_json[:_MAX_DETAILS_LEN]
        logger.debug("Audit details truncated to %d chars for action=%s", _MAX_DETAILS_LEN, action)

    try:
        await database.execute(
            "INSERT INTO audit_log (actor_id, action, target_type, target_id, details) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            user.get("sub", "unknown"),
            action,
            target_type,
            target_id,
            details_json,
        )
    except Exception as e:
        logger.error(
            "Audit log write failed [actor=%s action=%s target=%s/%s]: %s",
            user.get("sub", "?"),
            action,
            target_type,
            target_id,
            e,
        )