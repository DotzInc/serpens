import asyncio
import base64
import logging
import os
from abc import ABC, abstractmethod

from serpens.kafka.event_publisher import EventPublisher

try:
    from confluent_kafka import Consumer, KafkaError, Message
except ImportError:  # pragma: no cover
    Consumer = None  # type: ignore[assignment, misc]
    KafkaError = None  # type: ignore[assignment, misc]

logger = logging.getLogger(__name__)

IS_TESTING = os.getenv("IS_TESTING")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

class AsyncKafkaConsumerBase(ABC):
    def __init__(self, dlq_publisher: EventPublisher) -> None:
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._dlq_publisher = dlq_publisher

    async def start(self) -> None:
        """Starts the async polling loop as a background task."""
        if IS_TESTING:
            logger.info("Kafka consumer not started in test mode.")
            return

        if Consumer is None:
            logger.error("Kafka consumer not started: confluent_kafka unavailable.")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop())

        def _on_done(task: asyncio.Task):
            if not task.cancelled() and task.exception():
                logger.error("Consumer task crashed!", exc_info=task.exception())

        self._task.add_done_callback(_on_done)

    async def stop(self) -> None:
        """Gracefully stops the consumer task."""
        self._stop_event.set()
        if self._task:
            await self._task

    @property
    @abstractmethod
    def topic(self) -> str: ...

    @property
    @abstractmethod
    def topic_dlq(self) -> str: ...

    @abstractmethod
    async def process(self, message: str) -> None:
        """Subclasses must implement this natively async method."""
        pass

    async def _consume_loop(self) -> None:
        topic = self.topic.strip()
        servers = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]

        conf = {
            "bootstrap.servers": ",".join(servers),
            "group.id": KAFKA_CONSUMER_GROUP.strip(),
            "auto.offset.reset": "latest",
            "enable.auto.offset.store": False,
            "security.protocol": KAFKA_SECURITY_PROTOCOL,
        }

        consumer = Consumer(conf)
        consumer.subscribe([topic])
        logger.info(f"Async consumer started for topic '{topic}'")

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(0)

                if msg is None:
                    await asyncio.sleep(0.1)
                    continue

                if msg.error():
                    err = msg.error()
                    if err is not None and err.code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {err}")
                        await self._send_to_dlq(msg, "consumer_error", Exception(err))
                    continue

                try:
                    raw = msg.value()
                    value = raw.decode("utf-8") if raw else ""

                    await self.process(value)

                    consumer.store_offsets(msg)

                except Exception as exc:
                    logger.error(f"Failed to process message: {exc}")
                    await self._send_to_dlq(msg, "process_error", exc)
                    consumer.store_offsets(msg)

        finally:
            consumer.close()
            logger.info("Async consumer stopped.")

    async def _send_to_dlq(self, msg: Message, reason: str, error: Exception) -> None:
        dlq_topic = self.topic_dlq.strip()
        if not dlq_topic:
            return

        value_bytes = msg.value() if msg else None
        payload = {
            "source_topic": msg.topic() if msg else None,
            "partition": msg.partition() if msg else None,
            "offset": msg.offset() if msg else None,
            "reason": reason,
            "error": str(error),
            "value_base64": base64.b64encode(value_bytes).decode("utf-8") if value_bytes else None,
        }

        try:
            await self._dlq_publisher.publish(payload, dlq_topic)
            logger.info(f"Sent message to DLQ {dlq_topic}")
        except Exception as exc:
            logger.exception(f"Failed to publish to DLQ: {exc}")
