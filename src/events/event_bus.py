"""Event-driven architecture with message queue."""

import json
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Base event class."""
    event_type: str
    payload: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    event_id: str = ""
    
    def to_json(self) -> str:
        """Serialize event to JSON."""
        return json.dumps({
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "event_id": self.event_id
        })
    
    @classmethod
    def from_json(cls, data: str) -> "Event":
        """Deserialize event from JSON."""
        obj = json.loads(data)
        return cls(**obj)


class EventHandler(ABC):
    """Abstract event handler."""
    
    @abstractmethod
    async def handle(self, event: Event) -> None:
        """Handle an event."""
        pass


class EventBus:
    """In-memory event bus for local processing."""

    def __init__(self):
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """Subscribe handler to event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        logger.debug(f"Subscribed {handler.__class__.__name__} to {event_type}")

    async def publish(self, event: Event) -> None:
        """Publish event to bus."""
        await self._queue.put(event)
        logger.debug(f"Published event: {event.event_type}")

    async def start(self) -> None:
        """Start processing events."""
        self._running = True
        logger.info("Event bus started")
        
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=1.0
                )
                await self._dispatch(event)
            except asyncio.TimeoutError:
                continue

    async def _dispatch(self, event: Event) -> None:
        """Dispatch event to handlers."""
        handlers = self._handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                await handler.handle(event)
            except Exception as e:
                logger.error(f"Handler error: {e}")

    def stop(self) -> None:
        """Stop event bus."""
        self._running = False
        logger.info("Event bus stopped")


class RabbitMQEventBus:
    """Event bus using RabbitMQ."""

    def __init__(self, url: str, exchange: str = "events"):
        self.url = url
        self.exchange = exchange
        self._connection = None
        self._channel = None

    async def connect(self) -> None:
        """Connect to RabbitMQ."""
        try:
            import aio_pika
            self._connection = await aio_pika.connect_robust(self.url)
            self._channel = await self._connection.channel()
            await self._channel.declare_exchange(
                self.exchange,
                aio_pika.ExchangeType.TOPIC
            )
            logger.info("Connected to RabbitMQ")
        except ImportError:
            logger.warning("aio_pika not installed")

    async def publish(self, event: Event, routing_key: str = "") -> None:
        """Publish event to RabbitMQ."""
        if self._channel is None:
            raise RuntimeError("Not connected")
        
        import aio_pika
        exchange = await self._channel.get_exchange(self.exchange)
        await exchange.publish(
            aio_pika.Message(body=event.to_json().encode()),
            routing_key=routing_key or event.event_type
        )

    async def close(self) -> None:
        """Close connection."""
        if self._connection:
            await self._connection.close()
