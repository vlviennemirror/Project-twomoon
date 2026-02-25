import os
import secrets
import logging
import time
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from jose import jwt, JWTError

logger = logging.getLogger("twomoon.web_hub.auth")

DISCORD_API_BASE = "https://discord.com/api/v10"
DISCORD_AUTHORIZE_URL = "https://discord.com/api/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/oauth2/token"
DISCORD_SCOPES = "identify guilds.members.read"

DISCORD_CLIENT_ID = os.environ.get("DISCORD_CLIENT_ID", "")
DISCORD_CLIENT_SECRET = os.environ.get("DISCORD_CLIENT_SECRET", "")
DISCORD_REDIRECT_URI = os.environ.get("DISCORD_REDIRECT_URI", "")
JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_SECONDS = 43200

TWOMOON_GUILD_ID = os.environ.get("TWOMOON_GUILD_ID", "")
AUTHORIZED_ROLE_IDS: set[str] = set(
    filter(None, os.environ.get("AUTHORIZED_ROLE_IDS", "").split(","))
)

OWNER_ROLE_IDS: set[str] = set(
    filter(None, os.environ.get("OWNER_ROLE_IDS", "").split(","))
)
ADMIN_ROLE_IDS: set[str] = set(
    filter(None, os.environ.get("ADMIN_ROLE_IDS", "").split(","))
)

AUTH_COOKIE_NAME = "twomoon_auth_token"
STATE_COOKIE_NAME = "twomoon_oauth_state"
FRONTEND_SUCCESS_URL = os.environ.get("FRONTEND_SUCCESS_URL", "/dashboard")
FRONTEND_FAILURE_URL = os.environ.get("FRONTEND_FAILURE_URL", "/login?error=forbidden")


def _determine_clearance(member_role_ids: set[str]) -> str:
    if member_role_ids & OWNER_ROLE_IDS:
        return "owner"
    if member_role_ids & ADMIN_ROLE_IDS:
        return "admin"
    if member_role_ids & AUTHORIZED_ROLE_IDS:
        return "moderator"
    return "none"


def _mint_jwt(user_id: str, username: str, clearance: str, matched_roles: list[str]) -> str:
    now = int(time.time())
    claims = {
        "sub": user_id,
        "username": username,
        "clearance": clearance,
        "roles": matched_roles,
        "iat": now,
        "exp": now + JWT_EXPIRATION_SECONDS,
    }
    return jwt.encode(claims, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def _set_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=f"Bearer {token}",
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=JWT_EXPIRATION_SECONDS,
        path="/",
    )


def _clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(
        key=AUTH_COOKIE_NAME,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )


class OAuth2CookieBearer(OAuth2):

    def __init__(self, token_url: str = "/auth/login"):
        flows = OAuthFlowsModel(
            authorizationCode={
                "authorizationUrl": DISCORD_AUTHORIZE_URL,
                "tokenUrl": token_url,
            }
        )
        super().__init__(flows=flows, auto_error=False)

    async def __call__(self, request: Request) -> dict[str, Any]:
        auth_cookie: Optional[str] = request.cookies.get(AUTH_COOKIE_NAME)

        if not auth_cookie or not auth_cookie.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="Missing or malformed authentication cookie",
            )

        token = auth_cookie.split(" ", 1)[1]

        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            return payload
        except JWTError:
            raise HTTPException(
                status_code=401,
                detail="Token signature expired or tampered",
            )


oauth2_scheme = OAuth2CookieBearer()


async def get_current_user(
    user: dict[str, Any] = Depends(oauth2_scheme),
) -> dict[str, Any]:
    if not user.get("sub"):
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return user


def require_clearance(*allowed_levels: str):
    async def _dependency(
        user: dict[str, Any] = Depends(get_current_user),
    ) -> dict[str, Any]:
        if user.get("clearance") not in allowed_levels:
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient clearance. Required: {', '.join(allowed_levels)}",
            )
        return user
    return Depends(_dependency)


router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.get("/login")
async def login(request: Request) -> RedirectResponse:
    state = secrets.token_urlsafe(32)

    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": DISCORD_SCOPES,
        "state": state,
        "prompt": "none",
    }

    authorize_url = f"{DISCORD_AUTHORIZE_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

    response = RedirectResponse(url=authorize_url, status_code=302)
    response.set_cookie(
        key=STATE_COOKIE_NAME,
        value=state,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=300,
        path="/auth",
    )
    return response


@router.get("/callback")
async def callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
) -> RedirectResponse:
    if error:
        logger.warning("OAuth2 error from Discord: %s", error)
        return RedirectResponse(url=FRONTEND_FAILURE_URL, status_code=302)

    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state parameter")

    stored_state = request.cookies.get(STATE_COOKIE_NAME)
    if not stored_state or not secrets.compare_digest(stored_state, state):
        raise HTTPException(status_code=403, detail="Invalid OAuth2 state — possible CSRF attack")

    async with httpx.AsyncClient(timeout=10.0) as client:
        token_response = await _exchange_code(client, code)
        access_token = token_response.get("access_token")
        if not access_token:
            logger.error("Token exchange failed: %s", token_response)
            raise HTTPException(status_code=502, detail="Failed to obtain access token from Discord")

        user_data = await _fetch_discord_user(client, access_token)
        member_data = await _fetch_guild_member(client, access_token)

    member_role_ids = set(member_data.get("roles", []))
    matched_roles = list(member_role_ids & AUTHORIZED_ROLE_IDS)

    if not matched_roles:
        logger.warning(
            "Unauthorized login attempt: user=%s (%s) — no matching roles",
            user_data.get("id"),
            user_data.get("username"),
        )
        return RedirectResponse(url=FRONTEND_FAILURE_URL, status_code=302)

    clearance = _determine_clearance(member_role_ids)

    token = _mint_jwt(
        user_id=str(user_data["id"]),
        username=user_data.get("username", "unknown"),
        clearance=clearance,
        matched_roles=matched_roles,
    )

    logger.info(
        "Login successful: user=%s (%s) clearance=%s roles=%d",
        user_data["id"],
        user_data.get("username"),
        clearance,
        len(matched_roles),
    )

    response = RedirectResponse(url=FRONTEND_SUCCESS_URL, status_code=302)
    _set_auth_cookie(response, token)
    response.delete_cookie(key=STATE_COOKIE_NAME, path="/auth")
    return response


@router.post("/logout")
async def logout(response: Response) -> dict:
    _clear_auth_cookie(response)
    return {"status": "logged_out"}


@router.get("/me")
async def get_me(user: dict[str, Any] = Depends(get_current_user)) -> dict:
    return {
        "user_id": user.get("sub"),
        "username": user.get("username"),
        "clearance": user.get("clearance"),
        "roles": user.get("roles", []),
        "token_issued_at": user.get("iat"),
        "token_expires_at": user.get("exp"),
    }


async def _exchange_code(client: httpx.AsyncClient, code: str) -> dict:
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }

    try:
        resp = await client.post(
            DISCORD_TOKEN_URL,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error("Discord token exchange HTTP %d: %s", e.response.status_code, e.response.text)
        raise HTTPException(status_code=502, detail="Discord token exchange failed")
    except httpx.RequestError as e:
        logger.error("Discord token exchange network error: %s", e)
        raise HTTPException(status_code=502, detail="Cannot reach Discord API")


async def _fetch_discord_user(client: httpx.AsyncClient, access_token: str) -> dict:
    try:
        resp = await client.get(
            f"{DISCORD_API_BASE}/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error("Discord user fetch HTTP %d: %s", e.response.status_code, e.response.text)
        raise HTTPException(status_code=502, detail="Failed to fetch Discord user identity")
    except httpx.RequestError as e:
        logger.error("Discord user fetch network error: %s", e)
        raise HTTPException(status_code=502, detail="Cannot reach Discord API")


async def _fetch_guild_member(client: httpx.AsyncClient, access_token: str) -> dict:
    if not TWOMOON_GUILD_ID:
        raise HTTPException(status_code=500, detail="TWOMOON_GUILD_ID not configured")

    url = f"{DISCORD_API_BASE}/users/@me/guilds/{TWOMOON_GUILD_ID}/member"

    try:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if resp.status_code == 404:
            raise HTTPException(
                status_code=403,
                detail="You are not a member of the Two Moon server",
            )

        resp.raise_for_status()
        return resp.json()

    except HTTPException:
        raise
    except httpx.HTTPStatusError as e:
        logger.error("Discord member fetch HTTP %d: %s", e.response.status_code, e.response.text)
        raise HTTPException(status_code=502, detail="Failed to verify guild membership")
    except httpx.RequestError as e:
        logger.error("Discord member fetch network error: %s", e)
        raise HTTPException(status_code=502, detail="Cannot reach Discord API")
