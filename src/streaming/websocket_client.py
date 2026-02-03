"""WebSocket client with reconnection handling."""

import asyncio
from dataclasses import dataclass
from typing import Callable, Optional
import logging
import random

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """WebSocket connection configuration."""
    url: str
    heartbeat_interval: float = 30.0
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    max_reconnect_attempts: int = 10


class WebSocketClient:
    """Resilient WebSocket client with auto-reconnection."""

    def __init__(
        self,
        config: ConnectionConfig,
        on_message: Optional[Callable] = None
    ):
        self.config = config
        self.on_message = on_message
        self._connection = None
        self._running = False
        self._reconnect_attempts = 0
        self._last_pong = None

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        logger.info(f"Connecting to {self.config.url}")
        self._running = True
        
        while self._running:
            try:
                await self._establish_connection()
                self._reconnect_attempts = 0
                
                await asyncio.gather(
                    self._receive_loop(),
                    self._heartbeat_loop()
                )
            except ConnectionError as e:
                logger.warning(f"Connection lost: {e}")
                await self._handle_reconnect()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await self._handle_reconnect()

    async def _establish_connection(self) -> None:
        """Create WebSocket connection."""
        try:
            import websockets
            self._connection = await websockets.connect(
                self.config.url,
                ping_interval=None
            )
            logger.info("WebSocket connected")
        except ImportError:
            logger.warning("websockets not installed")
            raise ConnectionError("WebSocket library not available")

    async def _receive_loop(self) -> None:
        """Receive messages from WebSocket."""
        while self._running and self._connection:
            try:
                message = await asyncio.wait_for(
                    self._connection.recv(),
                    timeout=self.config.heartbeat_interval * 2
                )
                
                if message == "pong":
                    self._last_pong = asyncio.get_event_loop().time()
                elif self.on_message:
                    await self.on_message(message)
                    
            except asyncio.TimeoutError:
                logger.warning("Receive timeout, connection may be stale")
                raise ConnectionError("Receive timeout")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat pings."""
        while self._running and self._connection:
            try:
                await self._connection.send("ping")
                await asyncio.sleep(self.config.heartbeat_interval)
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                raise ConnectionError("Heartbeat failed")

    async def _handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff."""
        self._reconnect_attempts += 1
        
        if self._reconnect_attempts > self.config.max_reconnect_attempts:
            logger.error("Max reconnection attempts exceeded")
            self._running = False
            return
        
        delay = min(
            self.config.reconnect_base_delay * (2 ** self._reconnect_attempts),
            self.config.reconnect_max_delay
        )
        
        jitter = random.uniform(0, delay * 0.1)
        delay += jitter
        
        logger.info(f"Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts})")
        await asyncio.sleep(delay)

    async def disconnect(self) -> None:
        """Gracefully close connection."""
        self._running = False
        if self._connection:
            await self._connection.close()
            logger.info("WebSocket disconnected")

    async def send(self, message: str) -> None:
        """Send message through WebSocket."""
        if self._connection:
            await self._connection.send(message)
