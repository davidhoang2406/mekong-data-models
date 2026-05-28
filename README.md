# mekong-data-models

Shared schemas, message envelopes, and Kafka topic constants for the Mekong
data platform. This package is imported by `mekong-kafka`, `mekong-jobs`, and
`mekong-notebooks` — it does not run anything itself.

The schema here is the **contract** between producers and consumers. Breaking
changes require a major version bump and coordination across all dependents.

## Package contents

```
market_data_models/
  topics.py     # Kafka topic name constants
  message.py    # build_envelope() — common Kafka message wrapper
  schemas.py    # Avro (PriceSnapshot, KafkaEnvelope) + PyArrow (OHLCVBar) schemas
  coerce.py     # coerce_float, coerce_int, to_ts (input sanitisers)
  registry.py   # Confluent Schema Registry client + Avro serializer/deserializer
```

## Install

From another repo's `requirements.txt`:

```
git+https://github.com/davidhoang2406/mekong-data-models.git@main
```

For editable local development across sibling clones:

```bash
pip install -e ../mekong-data-models
```

## Topics

| Constant | Topic name |
|---|---|
| `STOCK_PRICE_REALTIME` | `stock.price.realtime` |
| `CRYPTO_PRICE_REALTIME` | `crypto.price.realtime` |

## Schemas

- **`PRICE_SNAPSHOT_AVRO_SCHEMA`** — row format written to
  `s3://market-data/price.snapshot/...avro` by the storage consumer.
- **`KAFKA_ENVELOPE_AVRO_SCHEMA`** — wire format registered with the
  Confluent Schema Registry for `*.price.realtime` topics.
- **`OHLCV_BAR_SCHEMA`** — PyArrow schema for the OHLCV Parquet output.

## Tests

```bash
pip install -e ".[dev]"
pytest
```

## Versioning

[Semver](https://semver.org/) — additive field = minor bump, renamed or
removed field = major bump. Downstream repos pin to a minor range
(`>=0.1,<0.2`) to take patches without breaking changes.
