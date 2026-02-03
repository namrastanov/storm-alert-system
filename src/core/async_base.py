"""Async utilities and base classes."""

import asyncio
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncService(ABC):
    """Base class for async services."""

    def __init__(self, name: str):
        self.name = name
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @abstractmethod
    async def _run(self) -> None:
        """Main service loop."""
        pass

    async def start(self) -> None:
        """Start the service."""
        if self._running:
            logger.warning(f"{self.name} already running")
            return
        
        logger.info(f"Starting {self.name}")
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the service gracefully."""
        if not self._running:
            return
        
        logger.info(f"Stopping {self.name}")
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running


class AsyncPool:
    """Pool of async workers."""

    def __init__(self, size: int = 10):
        self.size = size
        self._semaphore = asyncio.Semaphore(size)
        self._active = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        """Acquire a slot in the pool."""
        await self._semaphore.acquire()
        self._active += 1
        try:
            yield
        finally:
            self._active -= 1
            self._semaphore.release()

    @property
    def active_count(self) -> int:
        """Get number of active workers."""
        return self._active

    @property
    def available(self) -> int:
        """Get number of available slots."""
        return self.size - self._active


async def retry_async(
    coro_func,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
) -> T:
    """Retry async function with exponential backoff."""
    last_error = None
    
    for attempt in range(max_attempts):
        try:
            return await coro_func()
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                wait = delay * (backoff ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait}s")
                await asyncio.sleep(wait)
    
    raise last_error


async def gather_with_limit(coros, limit: int = 10):
    """Gather coroutines with concurrency limit."""
    semaphore = asyncio.Semaphore(limit)
    
    async def limited(coro):
        async with semaphore:
            return await coro
    
    return await asyncio.gather(*[limited(c) for c in coros])
