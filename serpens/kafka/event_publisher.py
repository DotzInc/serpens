import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from serpens.kafka.kafka_producer import KafkaProducer
from serpens.kafka.schemas import schema_registry

logger = logging.getLogger(__name__)
KAFKA_WAIT_FOR_DELIVERY_TIMEOUT = os.getenv("KAFKA_WAIT_FOR_DELIVERY_TIMEOUT")

class EventPublisher:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer

    async def publish(self, event_data: dict[str, Any], target_topic: str) -> None:
        key, payload = self._prepare_payload(event_data, target_topic)

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def delivery_report(err, msg):
            def resolve_future():
                if not future.done():
                    if err:
                        future.set_exception(Exception(err))
                    else:
                        future.set_result(msg)

            loop.call_soon_threadsafe(resolve_future)

        self.producer.produce(topic=target_topic, key=key, value=payload, callback=delivery_report)

        try:
            await asyncio.wait_for(future, timeout=KAFKA_WAIT_FOR_DELIVERY_TIMEOUT)
        except TimeoutError as exc:
            logger.error(f"Kafka timeout after {KAFKA_WAIT_FOR_DELIVERY_TIMEOUT}s.")
            raise RuntimeError("Kafka delivery timed out") from exc

    def _prepare_payload(
        self, event_data: dict[str, Any], target_topic: str
    ) -> tuple[bytes, bytes]:
        schema_info = schema_registry.get(target_topic)

        prepared_data = {
            **event_data,
            "internal_event_id": str(uuid.uuid4()),
            "event_type": schema_info.get("event_type", ""),
            "published_at": int(datetime.now(UTC).timestamp() * 1000),
        }

        message = {
            "schema": schema_info.get("schema", {}),
            "payload": prepared_data,
            "save_to_bucket": schema_info.get("save_to_bucket", False),
            "target_topic": target_topic,
        }

        key = str(prepared_data["published_at"]).encode("utf-8")
        payload = json.dumps(message, ensure_ascii=False).encode("utf-8")

        return key, payload
