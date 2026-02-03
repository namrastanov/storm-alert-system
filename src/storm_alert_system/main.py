"""Main application module for storm alert system."""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class StormAlertSystem:
    """Real-time storm monitoring and alert system."""

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self._running = False
        self._alert_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        """Start the alert monitoring system."""
        logger.info("Starting Storm Alert System...")
        self._running = True
        await asyncio.gather(
            self._monitor_weather_feeds(),
            self._process_alerts(),
            self._send_notifications()
        )

    async def stop(self) -> None:
        """Gracefully stop the system."""
        logger.info("Stopping Storm Alert System...")
        self._running = False

    async def _monitor_weather_feeds(self) -> None:
        """Monitor incoming weather data feeds."""
        while self._running:
            logger.debug("Checking weather feeds...")
            await asyncio.sleep(1)

    async def _process_alerts(self) -> None:
        """Process incoming alerts and prioritize them."""
        while self._running:
            try:
                alert = await asyncio.wait_for(
                    self._alert_queue.get(),
                    timeout=1.0
                )
                logger.info(f"Processing alert: {alert}")
            except asyncio.TimeoutError:
                continue

    async def _send_notifications(self) -> None:
        """Send notifications through configured channels."""
        while self._running:
            await asyncio.sleep(1)

    def add_alert(self, alert: dict) -> None:
        """Add an alert to the processing queue."""
        self._alert_queue.put_nowait(alert)


def create_app(config: Optional[dict] = None) -> StormAlertSystem:
    """Factory function to create system instance."""
    return StormAlertSystem(config)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(level=logging.INFO)
    system = create_app()
    try:
        asyncio.run(system.start())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")


if __name__ == "__main__":
    main()
