import fastavro
import pyarrow as pa

# ── Avro (streaming → StorageConsumer → MinIO market-data) ────────────────────

PRICE_SNAPSHOT_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record", "name": "PriceSnapshot",
    "fields": [
        {"name": "time",       "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "symbol",     "type": "string"},
        {"name": "exchange",   "type": "string"},
        {"name": "price",      "type": "double"},
        {"name": "change",     "type": "double"},
        {"name": "pct_change", "type": "double"},
        {"name": "volume",     "type": "long"},
        {"name": "bid",        "type": "double"},
        {"name": "ask",        "type": "double"},
    ],
})

# ── Avro (Kafka message envelope — Schema Registry) ──────────────────────────
# This is the schema registered with the Confluent Schema Registry for the
# Kafka message wire format. It mirrors the JSON envelope from message.py.

KAFKA_ENVELOPE_AVRO_SCHEMA_RAW = {
    "type": "record",
    "name": "KafkaEnvelope",
    "namespace": "com.mekong.market_data",
    "fields": [
        {"name": "event_type", "type": "string"},
        {"name": "symbol",     "type": "string"},
        {"name": "exchange",   "type": "string"},
        {"name": "timestamp",  "type": "string"},
        {"name": "source",     "type": "string"},
        {"name": "payload",    "type": {
            "type": "record",
            "name": "PricePayload",
            "fields": [
                {"name": "price",      "type": "double"},
                {"name": "change",     "type": "double"},
                {"name": "pct_change", "type": "double"},
                {"name": "volume",     "type": "long"},
                {"name": "bid",        "type": "double"},
                {"name": "ask",        "type": "double"},
            ],
        }},
    ],
}

KAFKA_ENVELOPE_AVRO_SCHEMA = fastavro.parse_schema(KAFKA_ENVELOPE_AVRO_SCHEMA_RAW)

# ── Parquet (batch ingest → MinIO market-analysis) ─────────────────────────────

OHLCV_BAR_SCHEMA = pa.schema([
    pa.field("time",     pa.timestamp("us", tz="UTC")),
    pa.field("symbol",   pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("open",     pa.float64()),
    pa.field("high",     pa.float64()),
    pa.field("low",      pa.float64()),
    pa.field("close",    pa.float64()),
    pa.field("volume",   pa.int64()),
])
