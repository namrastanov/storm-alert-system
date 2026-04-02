"""API rate limiting middleware"""

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Optional, Dict, Callable, Any, Awaitable
import logging

logger = logging.getLogger(__name__)

# Default eviction settings
_DEFAULT_BUCKET_TTL = 600  # seconds before an idle bucket is evicted
_DEFAULT_MAX_KEYS = 100_000  # hard cap on tracked keys
_DEFAULT_EVICTION_INTERVAL = 60  # seconds between eviction sweeps


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_minute: int = 60
    burst_size: int = 10
    block_duration_seconds: int = 300
    bucket_ttl_seconds: int = _DEFAULT_BUCKET_TTL
    max_keys: int = _DEFAULT_MAX_KEYS
    eviction_interval_seconds: int = _DEFAULT_EVICTION_INTERVAL


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    remaining: int
    reset_at: float
    retry_after: Optional[int] = None


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.rate
        )
        self._last_update = now

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    @property
    def tokens(self) -> float:
        """Get current token count."""
        self._refill()
        return self._tokens

    def time_to_next_token(self) -> float:
        """Return seconds until at least one token is available."""
        self._refill()
        if self._tokens >= 1:
            return 0.0
        return (1.0 - self._tokens) / self.rate


class RateLimiter:
    """In-memory rate limiter suitable for single-process deployments.

    For distributed environments, a Redis-backed implementation with atomic operations
    (e.g., MULTI/EXEC) should be used.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._buckets: Dict[str, TokenBucket] = {}
        self._blocked: Dict[str, float] = {}
        self._violations: Dict[str, tuple[int, float]] = {}  # (count, first_violation_time)
        self._last_access: Dict[str, float] = {}  # tracks last access time per key
        self._last_eviction: float = time.time()
        self._lock = asyncio.Lock()

    def _evict_stale(self) -> None:
        """Periodically remove stale buckets and expired blocks to bound memory."""
        now = time.time()
        if now - self._last_eviction < self.config.eviction_interval_seconds:
            return
        self._last_eviction = now

        ttl = self.config.bucket_ttl_seconds

        # Evict expired blocks
        expired_blocks = [k for k, v in self._blocked.items() if now >= v]
        for k in expired_blocks:
            del self._blocked[k]
            self._violations.pop(k, None)

        # Evict stale buckets that haven't been accessed within the TTL
        stale_keys = [k for k, v in self._last_access.items() if now - v > ttl]
        for k in stale_keys:
            self._buckets.pop(k, None)
            self._last_access.pop(k, None)
            # Do NOT remove from _blocked or _violations — they have separate expiration

        # Hard cap: if still over max_keys, evict oldest entries first
        if len(self._buckets) > self.config.max_keys:
            sorted_keys = sorted(self._last_access, key=self._last_access.get)
            excess = len(self._buckets) - self.config.max_keys
            for k in sorted_keys[:excess]:
                self._buckets.pop(k, None)
                self._last_access.pop(k, None)
                self._blocked.pop(k, None)
                self._violations.pop(k, None)

        async with self._lock:
            self._evict_stale()
            now = time.time()

            # Check if key is blocked
            if key in self._blocked:
                block_until = self._blocked[key]
                if now < block_until:
                    return RateLimitResult(
                        allowed=False,
                        remaining=0,
                        reset_at=block_until,
                        retry_after=int(block_until - now)
                    )
                # Block expired
                del self._blocked[key]
                self._violations.pop(key, None)

            bucket = self._get_bucket(key)
            allowed = bucket.consume()

            if not allowed:
                # Update violation count
                violation_count, first_violation_time = self._violations.get(key, (0, now))
                if now - first_violation_time > 60:  # violation window expired
                    violation_count = 0
                    first_violation_time = now
                violation_count += 1
                self._violations[key] = (violation_count, first_violation_time)

                if violation_count >= 2:  # Second violation within window → block
                    block_until = now + self.config.block_duration_seconds
                    self._blocked[key] = block_until
                    logger.warning(
                        f"Rate limit exceeded for {key[:8]}... (blocked for {self.config.block_duration_seconds}s)"
                        if len(key) > 8 else
                        f"Rate limit exceeded for {key} (blocked for {self.config.block_duration_seconds}s)"
                    )
                    retry_after = self.config.block_duration_seconds
                    reset_at = block_until
                else:
                    # First violation → warning only, no block
                    logger.warning(
                        f"Rate limit warning for {key[:8]}..."
                        if len(key) > 8 else
                        f"Rate limit warning for {key}"
                    )
                    retry_after = int(bucket.time_to_next_token())
                    reset_at = now + retry_after if retry_after > 0 else now
            else:
                # Request allowed, reset violation count
                self._violations.pop(key, None)
                retry_after = None
                reset_at = (now // 60 + 1) * 60  # next whole minute for per-minute rate limiting

            remaining = int(bucket.tokens) if bucket.tokens >= 0 else 0
            return RateLimitResult(
                allowed=allowed,
                remaining=remaining,
                reset_at=reset_at,
                retry_after=retry_after
            )

    def _get_bucket(self, key: str) -> TokenBucket:
        """Get or create bucket for key."""
        if key not in self._buckets:
            rate = self.config.requests_per_minute / 60.0
            self._buckets[key] = TokenBucket(rate, self.config.burst_size)
        self._last_access[key] = time.time()
        return self._buckets[key]

    def reset(self, key: str) -> None:
        """Reset limits for key."""
        self._buckets.pop(key, None)
        self._blocked.pop(key, None)
        self._violations.pop(key, None)
        self._last_access.pop(key, None)


def rate_limit_middleware(
    limiter: RateLimiter,
    key_func: Callable[[Any], str]
) -> Callable[[Any, Callable[[Any], Awaitable[Any]]], Awaitable[Any]]:
    """Create framework-agnostic ASGI rate limiting middleware."""
    async def middleware(request: Any, call_next: Callable[[Any], Awaitable[Any]]) -> Any:
        key = key_func(request)
        result = await limiter.check(key)

        if not result.allowed:
            body = json.dumps({"error": "Rate limit exceeded"}).encode("utf-8")
            headers = [
                (b"content-type", b"application/json"),
                (b"retry-after", str(result.retry_after).encode()),
                (b"x-ratelimit-remaining", b"0"),
                (b"x-ratelimit-reset", str(int(result.reset_at)).encode()),
            ]

            async def send_rate_limit_response(scope, receive, send):
                await send({
                    "type": "http.response.start",
                    "status": 429,
            from starlette.responses import JSONResponse
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"},
                headers={
                    "Retry-After": str(result.retry_after),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(result.reset_at))
                }
            )
        return response

    return middleware
