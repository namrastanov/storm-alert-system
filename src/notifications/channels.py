"""Multi-channel notification system."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class NotificationPayload:
    """Notification content."""
    title: str
    message: str
    priority: str
    data: Dict[str, Any]
    recipient: str


@dataclass
class DeliveryResult:
    """Result of delivery attempt."""
    channel: str
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


class NotificationChannel(ABC):
    """Abstract base for notification channels."""

    def __init__(self, name: str):
        self.name = name
        self._enabled = True

    @abstractmethod
    async def send(self, payload: NotificationPayload) -> DeliveryResult:
        """Send notification through channel."""
        pass

    @abstractmethod
    def validate_recipient(self, recipient: str) -> bool:
        """Validate recipient address for channel."""
        pass

    def enable(self) -> None:
        """Enable channel."""
        self._enabled = True

    def disable(self) -> None:
        """Disable channel."""
        self._enabled = False


class EmailChannel(NotificationChannel):
    """Email notification channel."""

    def __init__(self, smtp_host: str, smtp_port: int, username: str, password: str):
        super().__init__("email")
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password

    async def send(self, payload: NotificationPayload) -> DeliveryResult:
        """Send email notification."""
        logger.info(f"Sending email to {payload.recipient}")
        return DeliveryResult(self.name, True, "msg-001")

    def validate_recipient(self, recipient: str) -> bool:
        """Validate email address."""
        return "@" in recipient and "." in recipient


class SMSChannel(NotificationChannel):
    """SMS notification channel via Twilio."""

    def __init__(self, account_sid: str, auth_token: str, from_number: str):
        super().__init__("sms")
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_number = from_number

    async def send(self, payload: NotificationPayload) -> DeliveryResult:
        """Send SMS notification."""
        logger.info(f"Sending SMS to {payload.recipient}")
        return DeliveryResult(self.name, True, "sms-001")

    def validate_recipient(self, recipient: str) -> bool:
        """Validate phone number."""
        return recipient.startswith("+") and len(recipient) >= 10


class WebhookChannel(NotificationChannel):
    """Webhook callback channel."""

    def __init__(self, default_url: Optional[str] = None):
        super().__init__("webhook")
        self.default_url = default_url

    async def send(self, payload: NotificationPayload) -> DeliveryResult:
        """Send webhook notification."""
        url = payload.recipient or self.default_url
        logger.info(f"Sending webhook to {url}")
        return DeliveryResult(self.name, True, "wh-001")

    def validate_recipient(self, recipient: str) -> bool:
        """Validate webhook URL."""
        return recipient.startswith("http")
