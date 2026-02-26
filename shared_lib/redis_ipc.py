import os
import json
import asyncio
import logging
import time
from typing import Optional, Any, Callable, Awaitable

import redis.asyncio as redis

logger = logging.getLogger("twomoon.redis_ipc")

_redis_pool: Optional[redis.Redis] = None
_pool_lock = asyncio.Lock()

RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_MAX_ATTEMPTS = 0


async def get_redis() -> redis.Redis:
    global _redis_pool
    if _redis_pool is not None:
        return _redis_pool

    async with _pool_lock:
        if _redis_pool is not None:
            return _redis_pool

        url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

        _redis_pool = redis.from_url(
            url,
            decode_responses=True,
            max_connections=16,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
            retry_on_timeout=True,
            health_check_interval=20,
        )

        await _redis_pool.ping()
        logger.info("Redis connection pool initialized (%s)", url.split("@")[-1])
        return _redis_pool


async def close_redis() -> None:
    global _redis_pool
    if _redis_pool is not None:
        await _redis_pool.aclose()
        _redis_pool = None
        logger.info("Redis connection pool closed")


async def health_check() -> bool:
    try:
        r = await get_redis()
        return await r.ping()
    except (redis.RedisError, OSError):
        return False


async def cache_get(key: str) -> Optional[str]:
    r = await get_redis()
    return await r.get(key)


async def cache_set(key: str, value: str, ttl_seconds: Optional[int] = None) -> None:
    r = await get_redis()
    if ttl_seconds:
        await r.setex(key, ttl_seconds, value)
    else:
        await r.set(key, value)


async def cache_delete(*keys: str) -> int:
    if not keys:
        return 0
    r = await get_redis()
    return await r.delete(*keys)


async def cache_get_json(key: str) -> Optional[Any]:
    raw = await cache_get(key)
    if raw is None:
        return None
    return json.loads(raw)


async def cache_set_json(key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
    await cache_set(key, json.dumps(value, default=str), ttl_seconds)


async def cache_exists(key: str) -> bool:
    r = await get_redis()
    return bool(await r.exists(key))


async def publish_event(channel: str, event_type: str, payload: Optional[dict] = None) -> int:
    r = await get_redis()
    message = {
        "event_type": event_type,
        "payload": payload or {},
        "ts": time.time(),
    }
    subscriber_count = await r.publish(channel, json.dumps(message, default=str))
    logger.info(
        "Published [%s] to '%s' → %d subscriber(s)",
        event_type,
        channel,
        subscriber_count,
    )
    return subscriber_count


EventCallback = Callable[[str, dict], Awaitable[None]]


class Subscription:
    __slots__ = ("channel", "callback", "_task", "_pubsub", "_stop_event")

    def __init__(self, channel: str, callback: EventCallback):
        self.channel = channel
        self.callback = callback
        self._task: Optional[asyncio.Task] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> asyncio.Task:
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._listen_loop(),
            name=f"redis-sub:{self.channel}",
        )
        return self._task

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._cleanup_pubsub()

    async def _cleanup_pubsub(self) -> None:
        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe(self.channel)
                await self._pubsub.aclose()
            except Exception:
                pass
            self._pubsub = None

    async def _listen_loop(self) -> None:
        delay = RECONNECT_BASE_DELAY
        attempts = 0

        while not self._stop_event.is_set():
            try:
                r = await get_redis()
                self._pubsub = r.pubsub()
                await self._pubsub.subscribe(self.channel)
                logger.info("Subscribed to '%s'", self.channel)
                delay = RECONNECT_BASE_DELAY
                attempts = 0

                async for raw_message in self._pubsub.listen():
                    if self._stop_event.is_set():
                        break

                    if raw_message["type"] != "message":
                        continue

                    try:
                        envelope = json.loads(raw_message["data"])
                        event_type = envelope.get("event_type", "UNKNOWN")
                        payload = envelope.get("payload", {})
                    except (json.JSONDecodeError, TypeError, AttributeError) as e:
                        logger.warning(
                            "Malformed message on '%s': %s",
                            self.channel,
                            e,
                        )
                        continue

                    try:
                        await self.callback(event_type, payload)
                    except Exception:
                        logger.exception(
                            "Callback error for [%s] on '%s'",
                            event_type,
                            self.channel,
                        )

            except asyncio.CancelledError:
                break

            except (redis.RedisError, OSError, ConnectionError) as e:
                await self._cleanup_pubsub()
                attempts += 1

                if RECONNECT_MAX_ATTEMPTS and attempts >= RECONNECT_MAX_ATTEMPTS:
                    logger.error(
                        "Max reconnect attempts (%d) for '%s'. Giving up.",
                        RECONNECT_MAX_ATTEMPTS,
                        self.channel,
                    )
                    break

                logger.warning(
                    "Redis connection lost on '%s' (%s). Reconnecting in %.1fs (attempt %d)",
                    self.channel,
                    type(e).__name__,
                    delay,
                    attempts,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)

            except Exception:
                logger.exception("Fatal error in listener for '%s'", self.channel)
                await self._cleanup_pubsub()
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)

        await self._cleanup_pubsub()
        logger.info("Listener loop exited for '%s'", self.channel)


_active_subscriptions: dict[str, Subscription] = {}


async def subscribe_to_channel(channel: str, callback: EventCallback) -> asyncio.Task:
    if channel in _active_subscriptions:
        await _active_subscriptions[channel].stop()

    sub = Subscription(channel, callback)
    _active_subscriptions[channel] = sub
    task = await sub.start()
    return task


async def unsubscribe_from_channel(channel: str) -> None:
    sub = _active_subscriptions.pop(channel, None)
    if sub:
        await sub.stop()
        logger.info("Unsubscribed from '%s'", channel)


async def unsubscribe_all() -> None:
    channels = list(_active_subscriptions.keys())
    for channel in channels:
        await unsubscribe_from_channel(channel)
    logger.info("All subscriptions terminated (%d channels)", len(channels))


async def shutdown() -> None:
    await unsubscribe_all()
    await close_redis()
    logger.info("Redis IPC fully shut down")


def build_config_channel(bot_id: str) -> str:
    return f"events:config:{bot_id}"


def build_moderation_channel(bot_id: str) -> str:
    return f"events:moderation:{bot_id}"


def build_guild_channel(guild_id: str, namespace: str) -> str:
    return f"events:{namespace}:{guild_id}"
