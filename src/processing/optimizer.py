"""Optimized alert processing pipeline."""

import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
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

    def _get_cell(self, lat: float, lon: float) -> tuple:
        """Get grid cell for coordinates."""
        return (
            int(lat / self.resolution),
            int(lon / self.resolution)
        )

    def insert(self, item: dict, lat: float, lon: float) -> None:
        """Insert item into index."""
        cell = self._get_cell(lat, lon)
        if cell not in self._grid:
            self._grid[cell] = []
        self._grid[cell].append(item)

    def query_radius(
        self,
        lat: float,
        lon: float,
        radius_cells: int = 1
    ) -> List[dict]:
        """Query items within radius."""
        center = self._get_cell(lat, lon)
        results = []
        
        for dx in range(-radius_cells, radius_cells + 1):
            for dy in range(-radius_cells, radius_cells + 1):
                cell = (center[0] + dx, center[1] + dy)
                results.extend(self._grid.get(cell, []))
        
        return results

    def clear(self) -> None:
        """Clear the index."""
        self._grid.clear()


class BatchProcessor:
    """Batch process alerts for efficiency."""

    def __init__(
        self,
        batch_size: int = 100,
        flush_interval: float = 1.0,
        process_func: Optional[Callable] = None
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.process_func = process_func
        self._buffer: List = []
        self._last_flush = time.time()
- from concurrent.futures import ThreadPoolExecutor
+ from concurrent.futures import ThreadPoolExecutor
+ import threading

Either remove the unused BatchProcessor class, or refactor ProcessingOptimizer to actually use it with proper async/await patterns instead of ThreadPoolExecutor. The current design mixes two incompatible concurrency models.
+ self._lock = threading.Lock()

    async def add(self, item: dict) -> None:
        """Add item to batch."""
        async with self._lock:
            self._buffer.append(item)
            
            should_flush = (
                len(self._buffer) >= self.batch_size or
                time.time() - self._last_flush > self.flush_interval
            )
            
            if should_flush:
                await self._flush()

    async def _flush(self) -> None:
        """Process buffered items."""
        if not self._buffer:
            return
        
        batch = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = time.time()
        
        if self.process_func:
            await self.process_func(batch)
        
        logger.debug(f"Flushed batch of {len(batch)} items")


Consider implementing __enter__ and __exit__ methods to make ProcessingOptimizer a context manager, or add __del__ to ensure cleanup:

+ def __enter__(self):
+     return self
+ 
+ def __exit__(self, exc_type, exc_val, exc_tb):
+     self.shutdown()
+     return False
- def __init__(self, workers: int = 4):
- class GeoIndex:
+ import threading
+ class GeoIndex:
      """Spatial index for geographic queries."""
      def __init__(self, resolution: float = 0.1):
          self.resolution = resolution
          self._grid: Dict[tuple, List] = {}
- async def process_alerts(self, alerts: List[dict]) -> List[dict]:
+ async def process_alerts(self, alerts: List[dict]) -> List[dict]:
      """Process alerts with optimizations."""
      start_time = time.time()
      
-     loop = asyncio.get_event_loop()
-     results = await loop.run_in_executor(
+     results = await asyncio.get_running_loop().run_in_executor(
          self._executor,
          self._process_batch,
          alerts
      )

      def insert(self, item: dict, lat: float, lon: float) -> None:
          """Insert item into index."""
+         with self._lock:
              cell = self._get_cell(lat, lon)
              if cell not in self._grid:
                  self._grid[cell] = []
- def _process_single(self, alert: dict) -> dict:
+ def _process_single(self, alert: dict) -> dict:
      """Process single alert."""
+     processed_alert = alert.copy()
-     alert["processed"] = True
-     alert["processed_at"] = time.time()
-     return alert
+     processed_alert["processed"] = True
+     processed_alert["processed_at"] = time.time()
+     return processed_alert

      def query_radius(self, lat: float, lon: float, radius_cells: int = 1) -> List[dict]:
          """Query items within radius."""
+         with self._lock:
              center = self._get_cell(lat, lon)
              results = []
              for dx in range(-radius_cells, radius_cells + 1):
                  for dy in range(-radius_cells, radius_cells + 1):
                      cell = (center[0] + dx, center[1] + dy)
                      results.extend(self._grid.get(cell, []))
              return results
+     import threading
      self.workers = workers
      self._executor = ThreadPoolExecutor(max_workers=workers)
      self._metrics = ProcessingMetrics()
+     self._metrics_lock = threading.Lock()
      self._geo_index = GeoIndex()
      self._connection_pool: Dict = {}

- def _update_metrics(self, count: int, elapsed_ms: float) -> None:
+ def _update_metrics(self, count: int, elapsed_ms: float) -> None:
+     with self._metrics_lock:
          self._metrics.alerts_processed += count
          self._metrics.total_time_ms += elapsed_ms
          self._metrics.avg_latency_ms = elapsed_ms / count if count > 0 else 0
          self._metrics.throughput_per_sec = (count / elapsed_ms) * 1000 if elapsed_ms > 0 else 0

    def __init__(self, workers: int = 4):
        self.workers = workers
        self._executor = ThreadPoolExecutor(max_workers=workers)
        self._metrics = ProcessingMetrics()
        self._geo_index = GeoIndex()
        self._connection_pool: Dict = {}

    async def process_alerts(self, alerts: List[dict]) -> List[dict]:
        """Process alerts with optimizations."""
        start_time = time.time()
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self._executor,
            self._process_batch,
            alerts
        )
        
        elapsed = (time.time() - start_time) * 1000
        self._update_metrics(len(alerts), elapsed)
        
        return results

    def _process_batch(self, alerts: List[dict]) -> List[dict]:
        """Process batch in thread pool."""
        processed = []
        for alert in alerts:
            processed.append(self._process_single(alert))
        return processed

    def _process_single(self, alert: dict) -> dict:
        """Process single alert."""
        alert["processed"] = True
        alert["processed_at"] = time.time()
        return alert

    def _update_metrics(self, count: int, elapsed_ms: float) -> None:
        """Update processing metrics."""
        self._metrics.alerts_processed += count
        self._metrics.total_time_ms += elapsed_ms
        self._metrics.avg_latency_ms = elapsed_ms / count if count > 0 else 0
        self._metrics.throughput_per_sec = (count / elapsed_ms) * 1000 if elapsed_ms > 0 else 0

    def get_metrics(self) -> ProcessingMetrics:
        """Get current metrics."""
        return self._metrics

    def shutdown(self) -> None:
        """Shutdown executor."""
        self._executor.shutdown(wait=True)
