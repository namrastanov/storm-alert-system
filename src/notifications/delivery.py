"""Notification delivery management."""

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
import logging

from .channels import NotificationChannel, NotificationPayload, DeliveryResult

logger = logging.getLogger(__name__)


@dataclass
class DeliveryTask:
    """Task in delivery queue."""
    id: str
    payload: NotificationPayload
    channels: List[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    attempts: int = 0
    max_attempts: int = 3


@dataclass
class DeliveryReport:
    """Report of delivery attempt."""
    task_id: str
    results: List[DeliveryResult]
    completed_at: datetime
    total_channels: int
    successful: int
    failed: int


class DeliveryManager:
    """Manage notification delivery across channels."""

    def __init__(self):
        self._channels: Dict[str, NotificationChannel] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    def register_channel(self, channel: NotificationChannel) -> None:
        """Register notification channel."""
        self._channels[channel.name] = channel
        logger.info(f"Registered channel: {channel.name}")

    async def enqueue(self, task: DeliveryTask) -> None:
        """Add task to delivery queue."""
        await self._queue.put(task)
        logger.debug(f"Enqueued task: {task.id}")

    async def deliver(self, task: DeliveryTask) -> DeliveryReport:
        """Deliver notification through specified channels."""
        results = []
        
        for channel_name in task.channels:
            channel = self._channels.get(channel_name)
            if channel is None:
                results.append(DeliveryResult(
                    channel_name, False, error="Channel not found"
                ))
                continue
            
            if not channel.validate_recipient(task.payload.recipient):
                results.append(DeliveryResult(
                    channel_name, False, error="Invalid recipient"
                ))
                continue
            
            try:
                result = await channel.send(task.payload)
                results.append(result)
            except Exception as e:
                results.append(DeliveryResult(
                    channel_name, False, error=str(e)
                ))
        
        successful = sum(1 for r in results if r.success)
        
        return DeliveryReport(
            task_id=task.id,
            results=results,
            completed_at=datetime.utcnow(),
            total_channels=len(task.channels),
            successful=successful,
            failed=len(task.channels) - successful
        )

    async def process_queue(self) -> None:
        """Process delivery queue."""
        self._running = True
        
        while self._running:
            try:
                task = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=1.0
                )
                report = await self.deliver(task)
                logger.info(f"Delivered {task.id}: {report.successful}/{report.total_channels}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Queue processing error: {e}")

    def stop(self) -> None:
        """Stop queue processing."""
        self._running = False
