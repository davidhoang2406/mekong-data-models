"""Schema Registry integration — Confluent wire format serialization.

Confluent wire format:
  byte 0:    magic byte (0x00)
  bytes 1-4: schema ID (big-endian int32)
  bytes 5+:  Avro binary payload

This module wraps fastavro + a simple REST client for the Confluent Schema
Registry, keeping kafka-python as the transport layer.
"""

import io
import json
import logging
import os
import struct
from functools import lru_cache
from typing import Any

import fastavro
import urllib.request
import urllib.error

from market_data_models.schemas import KAFKA_ENVELOPE_AVRO_SCHEMA, KAFKA_ENVELOPE_AVRO_SCHEMA_RAW

log = logging.getLogger(__name__)

_MAGIC_BYTE = b"\x00"
_HEADER_FORMAT = ">bI"  # magic byte + 4-byte big-endian schema ID
_HEADER_SIZE = 5


class SchemaRegistryClient:
    """Minimal Confluent Schema Registry REST client."""

    def __init__(self, url: str | None = None):
        self._url = (url or os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")).rstrip("/")

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self._url}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    def register_schema(self, subject: str, schema: dict) -> int:
        """Register an Avro schema under the given subject. Returns the schema ID."""
        result = self._request("POST", f"/subjects/{subject}/versions", {
            "schemaType": "AVRO",
            "schema": json.dumps(schema),
        })
        schema_id = result["id"]
        log.info("Registered schema subject=%s id=%d", subject, schema_id)
        return schema_id

    @lru_cache(maxsize=64)
    def get_schema(self, schema_id: int) -> dict:
        """Fetch a schema by ID (cached)."""
        result = self._request("GET", f"/schemas/ids/{schema_id}")
        return fastavro.parse_schema(json.loads(result["schema"]))

    def get_latest_schema_id(self, subject: str) -> int | None:
        """Get the latest schema ID for a subject, or None if not registered."""
        try:
            result = self._request("GET", f"/subjects/{subject}/versions/latest")
            return result["id"]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise


def _get_or_register(client: SchemaRegistryClient, topic: str) -> int:
    """Get existing schema ID or register the envelope schema for a topic."""
    subject = f"{topic}-value"
    schema_id = client.get_latest_schema_id(subject)
    if schema_id is not None:
        return schema_id
    return client.register_schema(subject, KAFKA_ENVELOPE_AVRO_SCHEMA_RAW)


class AvroSerializer:
    """Serializes dicts to Confluent wire format (magic byte + schema ID + Avro)."""

    def __init__(self, topic: str, registry_url: str | None = None):
        self._client = SchemaRegistryClient(registry_url)
        self._schema_id = _get_or_register(self._client, topic)
        self._schema = KAFKA_ENVELOPE_AVRO_SCHEMA

    def __call__(self, value: dict) -> bytes:
        buf = io.BytesIO()
        buf.write(struct.pack(_HEADER_FORMAT, 0, self._schema_id))
        fastavro.schemaless_writer(buf, self._schema, value)
        return buf.getvalue()


class AvroDeserializer:
    """Deserializes Confluent wire format bytes back to dicts.

    Falls back to JSON for messages without the Avro magic byte, so the
    consumer can process both old JSON messages and new Avro messages during
    the migration window.
    """

    def __init__(self, registry_url: str | None = None):
        self._client = SchemaRegistryClient(registry_url)

    def __call__(self, data: bytes) -> dict:
        if len(data) < _HEADER_SIZE or data[0:1] != _MAGIC_BYTE:
            return json.loads(data.decode("utf-8"))

        _, schema_id = struct.unpack(_HEADER_FORMAT, data[:_HEADER_SIZE])
        schema = self._client.get_schema(schema_id)
        buf = io.BytesIO(data[_HEADER_SIZE:])
        return fastavro.schemaless_reader(buf, schema)
