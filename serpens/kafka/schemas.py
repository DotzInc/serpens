import uuid
from datetime import UTC, datetime
from enum import StrEnum, unique
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class CallbackPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    internal_event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(default="CallbackEvent")
    published_at: int = Field(
        default_factory=lambda: int(datetime.now(UTC).timestamp() * 1000),
        description="org.apache.kafka.connect.data.Timestamp",
    )


PayloadT = TypeVar("PayloadT")


class KafkaMessageEnvelope(BaseModel, Generic[PayloadT]):
    schema_info: dict | None = Field(None, alias="schema")
    payload: PayloadT
    save_to_bucket: bool = False
    target_topic: str


def generate_kafka_connect_schema(model: type[BaseModel], schema_name: str) -> dict[str, Any]:
    type_mapping = {
        "str": "string",
        "int": "int64",
        "float": "double",
        "bool": "boolean",
    }

    fields = []
    for field_name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        py_type = (
            annotation.__name__
            if annotation is not None and hasattr(annotation, "__name__")
            else "str"
        )

        kafka_field = {
            "field": field_name,
            "type": type_mapping.get(py_type, "string"),
            "optional": not field_info.is_required(),
        }
        if field_info.description and "org.apache.kafka" in field_info.description:
            kafka_field["name"] = field_info.description

        fields.append(kafka_field)

    return {"type": "struct", "name": schema_name, "fields": fields}


class SchemaRegistry:
    def __init__(self) -> None:
        self._schemas: dict[str, dict[str, Any]] = {}

    def register(
        self,
        topic: str,
        model: type[BaseModel],
        event_type: str,
        save_to_bucket: bool = False,
    ) -> None:
        self._schemas[topic] = {
            "schema": generate_kafka_connect_schema(model, event_type),
            "event_type": event_type,
            "save_to_bucket": save_to_bucket,
        }

    def get(self, topic: str) -> dict[str, Any]:
        return self._schemas.get(topic, {})


schema_registry = SchemaRegistry()
