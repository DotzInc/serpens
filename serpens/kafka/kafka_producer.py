import asyncio
import logging
import os
from typing import Any, Protocol

try:  # pragma: no cover - optional dependency in tests
    from confluent_kafka import Producer as ConfluentProducer
except ImportError:  # pragma: no cover
    ConfluentProducer = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
RUN_KAFKA = os.getenv("RUN_KAFKA", "true").lower() == "true"
KAFKA_MESSAGE_TIMEOUT_MS = os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "10000")
IS_TESTING = os.getenv("IS_TESTING")
KAFKA_CLIENT_DEBUG_LOGGING = os.getenv("KAFKA_CLIENT_DEBUG_LOGGING", "false").lower() == "true"
KAFKA_RETRIES= os.getenv("KAFKA_RETRIES", "3"),

class DummyMessage:
    def __init__(self, topic: str, key: bytes, partition: int = 0, offset: int = 0):
        self._topic = topic
        self._key = key
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class DummyProducer:
    def __init__(self):
        self.messages: list[dict[str, Any]] = []

    def produce(
        self,
        topic: str,
        key: bytes | None = None,
        value: bytes | None = None,
        **kwargs: Any,
    ):
        message = {"topic": topic, "key": key, "value": value, "kwargs": kwargs}
        self.messages.append(message)

        cb = kwargs.get("callback") or kwargs.get("on_delivery")

        if cb:
            cb(None, DummyMessage(topic, key or b""))

    def poll(self, timeout: float):
        return 0

    def flush(self, timeout: float | None = None):
        return 0


class KafkaProducer(Protocol):
    def produce(
        self, topic: str, value: bytes | None = None, key: bytes | None = None, **kwargs: Any
    ): ...
    def poll(self, timeout: float): ...
    def flush(self, timeout: float): ...


class KafkaProducerProvider:
    def __init__(self):
        self._producer = None
        self._stop_event = None
        self._poller_task = None

    def __call__(self) -> Any:
        if self._producer is None:
            self._producer = self._create_producer()
        return self._producer

    def _create_producer(self) -> Any:
        if not RUN_KAFKA:
            return None

        if IS_TESTING:
            return DummyProducer()

        if ConfluentProducer is None:
            raise RuntimeError("confluent_kafka package is required to publish events")

        if not KAFKA_BOOTSTRAP_SERVERS:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not configured")

        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "message.timeout.ms": KAFKA_MESSAGE_TIMEOUT_MS,
            "retries": KAFKA_RETRIES,
            "logger": logger,
        }
        if KAFKA_CLIENT_DEBUG_LOGGING:
            conf["debug"] = "broker,topic,msg"
            conf["error_cb"] = lambda err: logger.warning("Kafka error: %s", err)

        return ConfluentProducer(conf)

    async def start_poller(self):
        self._stop_event = asyncio.Event()
        self()
        self._poller_task = asyncio.create_task(self._poll_loop())

        def _on_poller_done(task: asyncio.Task):
            if not task.cancelled() and task.exception():
                logger.error(
                    "FATAL: Kafka background poller task crashed.", exc_info=task.exception()
                )

        self._poller_task.add_done_callback(_on_poller_done)

    async def _poll_loop(self):
        while not self._stop_event.is_set():
            try:
                if self._producer:
                    self._producer.poll(0)
            except Exception as e:
                logger.error(f"Error while polling Kafka: {e}")

            await asyncio.sleep(0.05)

    async def shutdown(self):
        if self._stop_event:
            self._stop_event.set()
            if self._poller_task:
                await self._poller_task

        if self._producer and hasattr(self._producer, "flush"):
            self._producer.flush(10)
