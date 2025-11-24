# app/kafka_producer.py
import os
import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Don't connect at import time
_producer = None


def get_producer():
    """
    Lazily create a KafkaProducer with retries.
    This avoids crashing the app if the broker isn't ready yet.
    """
    global _producer
    if _producer is not None:
        return _producer

    attempts = 0
    max_attempts = 10
    while attempts < max_attempts and _producer is None:
        attempts += 1
        try:
            logger.info(
                f"[PRODUCER] Connecting to Kafka at {BOOTSTRAP_SERVERS} "
                f"(attempt {attempts}/{max_attempts})"
            )
            _producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("[PRODUCER] Connected to Kafka")
            return _producer
        except NoBrokersAvailable as e:
            logger.warning(
                f"[PRODUCER] No brokers available yet: {e}. "
                "Retrying in 5 seconds..."
            )
            time.sleep(5)

    raise RuntimeError("Kafka producer not available after retries")


def publish_event(key: str, value: dict):
    producer = get_producer()
    logger.info(
        f"[PRODUCER] Sending event to 'external-transfers' "
        f"key={key}, value={value}"
    )
    future = producer.send(
        "external-transfers",
        key=key.encode("utf-8"),
        value=value
    )
    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            f"[PRODUCER] Event sent to "
            f"{record_metadata.topic}[{record_metadata.partition}]@"
            f"{record_metadata.offset}"
        )
    except Exception as e:
        logger.exception(f"[PRODUCER] Failed to send event: {e}")
        raise
