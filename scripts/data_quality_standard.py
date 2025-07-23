from itertools import count
from click import group
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os
import json
import re
import logging
from datetime import datetime
import uuid
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, topic='generated_customers', bootstrap_servers='kafka:9092'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        group_id = str(uuid.uuid4())
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'earliest',
            "group.id": group_id,
        })
        self.consumer.subscribe([self.topic])
        self.seen_transaction_ids = set()
        self.CCCD_REGEX = r'^\d{12}$'
        self.VALID_TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer', 'payment']


    def check_data_quality(self, max_messages=100):
        try:
            count = 0
            while count < max_messages:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                count += 1
                logger.info(f"Received message: {msg.value().decode('utf-8')}")
                data = json.loads(msg.value().decode('utf-8'))
                
                logger.info(f"data received: {data}")

                issues = self.validate_data_quality(data)
                if issues:
                    logger.warning(f"Data quality issues found: {issues}")
                else:
                    logger.info("Data passed all quality checks.")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def validate_data_quality(self, tx):
        issues = []
        # Null/Required field check
        customer = tx.get('customer', {})
        account = tx.get('account', {})
        device = tx.get('device', {})
        transactions = tx.get('transactions', [])
        risks = tx.get('risks', [])
        auth_logs = tx.get('auth_logs', [])

        # Validate customer fields
        for field in ['customer_id', 'full_name', 'year_of_birth', 'phone']:
            if not customer.get(field):
                issues.append(f"Missing or null field: {field}")

        # CCCD format check
        if not re.fullmatch(self.CCCD_REGEX, str(customer.get('customer_id', ''))):
            issues.append("Invalid CCCD format: should be 12 digits")

        # Validate account fields
        for field in ['account_id', 'account_type', 'balance']:
            if not account.get(field):
                issues.append(f"Missing or null field: {field}")

        # Validate device fields
        for field in ['device_id', 'os']:
            if not device.get(field):
                issues.append(f"Missing or null field: {field}")

        # Validate transaction fields
        for txn in transactions:
            for field in ['transaction_id', 'amount', 'transaction_type']:
                if txn.get(field) in [None, '']:
                    issues.append(f"Missing or null field: {field}")
            if txn.get('transaction_type') not in self.VALID_TRANSACTION_TYPES:
                issues.append(f"Invalid transaction_type: {txn.get('transaction_type')}")
            if not isinstance(txn.get('amount'), (int, float)) or txn.get('amount') <= 0:
                issues.append("Invalid or missing amount")
            txid = txn.get('transaction_id')
            if txid in self.seen_transaction_ids:
                issues.append(f"Duplicate transaction_id: {txid}")
            else:
                self.seen_transaction_ids.add(txid)
        


        return issues
