"""API rate limiting middleware."""

import time
from dataclasses import dataclass, field
from typing import Optional, Dict
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
    requests_per_hour: int = 1000
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
        self._tokens = capacity
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
    def tokens(self) -> int:
        """Get current token count."""
        self._refill()
        return int(self._tokens)


class RateLimiter:
    """Rate limiter with per-key tracking."""

    def __init__(self, config: RateLimitConfig, redis_url: Optional[str] = None):
        self.config = config
        self.redis_url = redis_url
        self._buckets: Dict[str, TokenBucket] = {}
        self._blocked: Dict[str, float] = {}
        self._last_access: Dict[str, float] = {}  # tracks last access time per key
        self._last_eviction: float = time.time()

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

        # Evict idle buckets (not accessed within TTL)
        stale_keys = [
            k for k, last in self._last_access.items()
            if now - last > ttl
        ]
        for k in stale_keys:
            self._buckets.pop(k, None)
            self._last_access.pop(k, None)
            self._blocked.pop(k, None)

        # Hard cap: if still over max_keys, evict oldest entries first
        if len(self._buckets) > self.config.max_keys:
            sorted_keys = sorted(self._last_access, key=self._last_access.get)
            excess = len(self._buckets) - self.config.max_keys
            for k in sorted_keys[:excess]:
                self._buckets.pop(k, None)
                self._last_access.pop(k, None)
                self._blocked.pop(k, None)

    def _get_bucket(self, key: str) -> TokenBucket:
        """Get or create bucket for key."""
        if key not in self._buckets:
            rate = self.config.requests_per_minute / 60.0
            self._buckets[key] = TokenBucket(rate, self.config.burst_size)
        self._last_access[key] = time.time()
        return self._buckets[key]

    def check(self, key: str) -> RateLimitResult:
        """Check if request is allowed."""
        # Run periodic eviction to bound memory
        self._evict_stale()

        if key in self._blocked:
            block_until = self._blocked[key]
            if time.time() < block_until:
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=block_until,
                    retry_after=int(block_until - time.time())
                )
            del self._blocked[key]
        
        bucket = self._get_bucket(key)
        allowed = bucket.consume()
        
        if not allowed:
            self._blocked[key] = time.time() + self.config.block_duration_seconds
            logger.warning(f"Rate limit exceeded for {key[:8]}..." if len(key) > 8 else f"Rate limit exceeded for {key}")
        
        return RateLimitResult(
            allowed=allowed,
            remaining=bucket.tokens,
            reset_at=time.time() + 60,
            retry_after=60 if not allowed else None
        )

    def reset(self, key: str) -> None:
        """Reset limits for key."""
        self._buckets.pop(key, None)
        self._blocked.pop(key, None)
        self._last_access.pop(key, None)


def rate_limit_middleware(limiter: RateLimiter, key_func):
    """Create rate limiting middleware."""
    async def middleware(request, call_next):
        key = key_func(request)
        result = limiter.check(key)
        
        if not result.allowed:
# Consider adding fastapi to setup.py install_requires,
# or return a framework-agnostic response object.
# For now, if keeping fastapi:
from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"},
                headers={
                    "Retry-After": str(result.retry_after),
                    "X-RateLimit-Remaining": "0"
                }
            )
        
        response = await call_next(request)
        response.headers["X-RateLimit-Remaining"] = str(result.remaining)
        return response
    
    return middleware