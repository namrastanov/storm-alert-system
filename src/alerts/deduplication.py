"""Alert deduplication with distributed locking."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Set
import logging

logger = logging.getLogger(__name__)


@dataclass
class AlertFingerprint:
    """Unique identifier for an alert."""
    alert_type: str
    location_hash: str
    severity: str
    time_bucket: str
    
    def __hash__(self) -> int:
        return hash((self.alert_type, self.location_hash, self.severity, self.time_bucket))
    
    def to_string(self) -> str:
        """Convert to string key."""
        return f"{self.alert_type}:{self.location_hash}:{self.severity}:{self.time_bucket}"


class AlertDeduplicator:
    """Deduplication for weather alerts."""

    BUCKET_MINUTES = 15
    CACHE_TTL_HOURS = 24

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url
        self._local_cache: Set[str] = set()
        self._redis_client = None
        self._stats = {"processed": 0, "duplicates": 0}

    def _get_redis(self):
        """Get Redis client."""
        if self._redis_client is None and self.redis_url:
            try:
                import redis
                self._redis_client = redis.from_url(self.redis_url)
            except ImportError:
                logger.warning("Redis not available, using local cache")
        return self._redis_client

    def _create_fingerprint(self, alert: dict) -> AlertFingerprint:
        """Create fingerprint for alert."""
        location = f"{alert.get('latitude', 0):.2f},{alert.get('longitude', 0):.2f}"
        location_hash = hashlib.md5(location.encode()).hexdigest()[:8]
        
        timestamp = datetime.fromisoformat(alert.get("timestamp", datetime.utcnow().isoformat()))
        bucket = timestamp.replace(
            minute=(timestamp.minute // self.BUCKET_MINUTES) * self.BUCKET_MINUTES,
            second=0,
            microsecond=0
        )
        
        return AlertFingerprint(
            alert_type=alert.get("type", "unknown"),
            location_hash=location_hash,
            severity=alert.get("severity", "unknown"),
            time_bucket=bucket.isoformat()
        )

    def is_duplicate(self, alert: dict) -> bool:
        """Check if alert is duplicate."""
        self._stats["processed"] += 1
        fingerprint = self._create_fingerprint(alert)
        key = fingerprint.to_string()
        
        redis = self._get_redis()
        if redis:
            try:
                if redis.exists(f"alert:fp:{key}"):
                    self._stats["duplicates"] += 1
                    return True
                redis.setex(
                    f"alert:fp:{key}",
                    timedelta(hours=self.CACHE_TTL_HOURS),
                    "1"
                )
                return False
            except Exception as e:
                logger.error(f"Redis error: {e}")
        
        if key in self._local_cache:
            self._stats["duplicates"] += 1
            return True
        
        self._local_cache.add(key)
        return False

    def clear_cache(self) -> None:
        """Clear local cache."""
        self._local_cache.clear()

    @property
    def duplicate_rate(self) -> float:
        """Get duplicate rate."""
        if self._stats["processed"] == 0:
            return 0.0
        return self._stats["duplicates"] / self._stats["processed"]

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "duplicate_rate": self.duplicate_rate,
            "cache_size": len(self._local_cache)
        }
