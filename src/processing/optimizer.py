"""Optimized alert processing pipeline."""

import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional
import time
import logging

logger = logging.getLogger(__name__)


@dataclass
class ProcessingMetrics:
    """Processing performance metrics."""
    alerts_processed: int = 0
    total_time_ms: float = 0
    avg_latency_ms: float = 0
    throughput_per_sec: float = 0
    cache_hits: int = 0
    cache_misses: int = 0


class GeoIndex:
    """Spatial index for geographic queries."""

    def __init__(self, resolution: float = 0.1):
        self.resolution = resolution
        self._grid: Dict[tuple, List] = {}
        self._lock = asyncio.Lock()

    def _get_cell(self, lat: float, lon: float) -> tuple:
        """Get grid cell for coordinates."""
        return (
            int(lat / self.resolution),
            int(lon / self.resolution)
        )

    async def insert(self, item: dict, lat: float, lon: float) -> None:
        """Insert item into index."""
        async with self._lock:
            cell = self._get_cell(lat, lon)
            if cell not in self._grid:
                self._grid[cell] = []
            self._grid[cell].append(item)

    async def query_radius(
        self,
        lat: float,
        lon: float,
        radius_cells: int = 1
    ) -> List[dict]:
        """Query items within radius."""
        async with self._lock:
            center = self._get_cell(lat, lon)
            results = []
            
            for dx in range(-radius_cells, radius_cells + 1):
                for dy in range(-radius_cells, radius_cells + 1):
                    cell = (center[0] + dx, center[1] + dy)
                    results.extend(self._grid.get(cell, []))
            
            return results

    async def clear(self) -> None:
        """Clear the index."""
        async with self._lock:
            self._grid.clear()


class ProcessingOptimizer:
    """Optimized alert processor with async-native implementation."""

    def __init__(self, workers: int = 4):
        self.workers = workers
        self._metrics = ProcessingMetrics()
        self._metrics_lock = asyncio.Lock()
        self._geo_index = GeoIndex()
        self._connection_pool: Dict = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def process_alerts(self, alerts: List[dict]) -> List[dict]:
        """Process alerts with optimizations."""
        start_time = time.time()
        
        # Process alerts concurrently using asyncio.gather
        tasks = [self._process_single(alert) for alert in alerts]
        results = await asyncio.gather(*tasks)
        
        elapsed = (time.time() - start_time) * 1000
        await self._update_metrics(len(alerts), elapsed)
        
        return results

    async def _process_single(self, alert: dict) -> dict:
        """Process single alert."""
        processed_alert = alert.copy()
        processed_alert["processed"] = True
        processed_alert["processed_at"] = time.time()
        
        # Example: Insert into geo index if coordinates exist
        if "lat" in alert and "lon" in alert:
            await self._geo_index.insert(processed_alert, alert["lat"], alert["lon"])
        
        return processed_alert

    async def _update_metrics(self, count: int, elapsed_ms: float) -> None:
        """Update processing metrics."""
        async with self._metrics_lock:
            self._metrics.alerts_processed += count
            self._metrics.total_time_ms += elapsed_ms
            self._metrics.avg_latency_ms = elapsed_ms / count if count > 0 else 0
            self._metrics.throughput_per_sec = (count / elapsed_ms) * 1000 if elapsed_ms > 0 else 0

    async def get_metrics(self) -> ProcessingMetrics:
        """Get current metrics."""
        async with self._metrics_lock:
            return self._metrics