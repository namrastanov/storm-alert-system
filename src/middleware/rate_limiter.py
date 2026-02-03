"""API rate limiting middleware."""

import time
from dataclasses import dataclass
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_minute: int = 60
    requests_per_hour: int = 1000
    burst_size: int = 10
    block_duration_seconds: int = 300


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

    def _get_bucket(self, key: str) -> TokenBucket:
        """Get or create bucket for key."""
        if key not in self._buckets:
            rate = self.config.requests_per_minute / 60.0
            self._buckets[key] = TokenBucket(rate, self.config.burst_size)
        return self._buckets[key]

    def check(self, key: str) -> RateLimitResult:
        """Check if request is allowed."""
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
            logger.warning(f"Rate limit exceeded for {key}")
        
        return RateLimitResult(
            allowed=allowed,
            remaining=bucket.tokens,
            reset_at=time.time() + 60,
            retry_after=60 if not allowed else None
        )

    def reset(self, key: str) -> None:
        """Reset limits for key."""
        if key in self._buckets:
            del self._buckets[key]
        if key in self._blocked:
            del self._blocked[key]


def rate_limit_middleware(limiter: RateLimiter, key_func):
    """Create rate limiting middleware."""
    async def middleware(request, call_next):
        key = key_func(request)
        result = limiter.check(key)
        
        if not result.allowed:
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
