from kafka import KafkaConsumer
import json
from app.db import get_db
from app import models


TOPIC = "bank-transactions"


def start_consumer(bank_id: str):

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"group-{bank_id}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print(f"🔄 Kafka consumer started for bank {bank_id}")

    for message in consumer:
        event = message.value

        # Ignore irrelevant events
        if event.get("destination_bank") != bank_id:
            continue

        db = next(get_db())

        account = db.query(models.Account).filter(
            models.Account.account_number == event["to_account"],
            models.Account.bank_id == bank_id
        ).first()

        if not account:
            print(f"⚠ No account for incoming transfer in bank {bank_id}")
            continue

        # Apply deposit
        account.balance += event["amount"]

        tx = models.Transaction(
            tx_id=event["tx_id"],
            tx_type="external_transfer_received",
            amount=event["amount"],
            origin_bank=event["from_bank"],
            destination_bank=bank_id,
            account_id=account.id
        )
        db.add(tx)
        db.commit()

        print(f"💰 Bank {bank_id}: Received {event['amount']} into {account.account_number}")
