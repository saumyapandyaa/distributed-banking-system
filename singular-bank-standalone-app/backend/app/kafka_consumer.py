# app/kafka_consumer.py
import os
import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from app.db import SessionLocal
from app import models

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def create_consumer(bank_id: str) -> KafkaConsumer:
    attempts = 0
    max_attempts = 10
    while True:
        attempts += 1
        try:
            logger.info(
                f"[{bank_id}] Creating KafkaConsumer for 'external-transfers' "
                f"on {BOOTSTRAP_SERVERS} (attempt {attempts}/{max_attempts})"
            )
            consumer = KafkaConsumer(
                "external-transfers",
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                group_id=f"bank-{bank_id}-external-consumer",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            logger.info(f"[{bank_id}] Kafka consumer READY")
            return consumer
        except NoBrokersAvailable as e:
            if attempts >= max_attempts:
                logger.error(
                    f"[{bank_id}] Kafka still not available after {max_attempts} "
                    f"attempts: {e}. Will keep retrying every 10s."
                )
                attempts = max_attempts  # stay at max, just log once
            else:
                logger.warning(
                    f"[{bank_id}] No brokers available: {e}. "
                    "Retrying in 5 seconds..."
                )
            time.sleep(5)
        except Exception as e:
            logger.exception(
                f"[{bank_id}] Unexpected error creating consumer: {e}. "
                "Retrying in 5 seconds..."
            )
            time.sleep(5)


def start_consumer(bank_id: str):
    """
    Run a Kafka consumer loop that listens for external transfer events
    and credits the destination account in *this* bank.
    """
    consumer = create_consumer(bank_id)

    for msg in consumer:
        event = msg.value
        logger.info(f"[{bank_id}] Received event: {event}")

        dest_bank = event.get("destination_bank")
        if dest_bank != bank_id:
            logger.info(
                f"[{bank_id}] Skipping event for destination_bank={dest_bank}"
            )
            continue

        to_account = event.get("to_account")
        amount = event.get("amount")

        if to_account is None or amount is None:
            logger.warning(f"[{bank_id}] Invalid event payload: {event}")
            continue

        db = SessionLocal()
        try:
            acct = db.query(models.Account).filter(
                models.Account.account_number == to_account,
                models.Account.bank_id == bank_id,
            ).first()

            if not acct:
                logger.warning(
                    f"[{bank_id}] Destination account not found: {to_account}"
                )
                db.close()
                continue

            old_balance = acct.balance
            acct.balance += amount

            tx = models.Transaction(
                tx_id=event["tx_id"],
                tx_type="external_transfer_received",
                amount=amount,
                origin_bank=event["from_bank"],
                destination_bank=bank_id,
                account_id=acct.id,
            )

            db.add(tx)
            db.commit()
            logger.info(
                f"[{bank_id}] Credited {amount} to account {to_account} "
                f"(old={old_balance}, new={acct.balance})"
            )
        except Exception as e:
            db.rollback()
            logger.exception(f"[{bank_id}] Error processing event: {e}")
        finally:
            db.close()
