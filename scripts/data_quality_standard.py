from itertools import count
from click import group
from confluent_kafka import Consumer, KafkaError
import psycopg2

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
            'auto.offset.reset': 'latest',
            "group.id": group_id,
        })
        self.consumer.subscribe([self.topic])
        self.seen_transaction_ids = set()
        self.valid_record = []
        self.CCCD_REGEX = r'^\d{12}$'
        self.VALID_TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer', 'payment']


    def check_data_quality_and_add_to_db(self, max_messages=50):
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
                    logger.info("Data quality check passed.")
                    try :
                        logger.info("Inserting valid data into database.")
                        self.insert_valid_data_to_db(data)
                        self.valid_record.append(data)
                    except Exception as e:
                        logger.error(f"Error inserting data into database: {e}")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def validate_data_quality(self, tx):
        issues = []
        # Getter
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
        for field in ['device_id', 'os', 'verified']:
            if not device.get(field):
                issues.append(f"Missing or null field: {field}")

        # Validate transaction fields
        for txn in transactions:
            for field in ['transaction_id', 'amount', 'transaction_type','receiver_id', 'transaction_date']:
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
        
        # Check foreign key relationships
        # 1. Customer ↔ Account
        if account.get('customer_id') != customer.get('customer_id'):
            issues.append("Foreign key mismatch: account.customer_id != customer.customer_id")

        # 2. Transaction ↔ Account
        for txn in transactions:
            if txn.get('account_id') != account.get('account_id'):
                issues.append(f"Foreign key mismatch: transaction.account_id != account.account_id ({txn.get('transaction_id')})")

        # 3. Transaction ↔ Device
        for txn in transactions:
            if txn.get('device_id') != device.get('device_id'):
                issues.append(f"Foreign key mismatch: transaction.device_id != device.device_id ({txn.get('transaction_id')})")

        # 4. Risk ↔ Transaction
        transaction_ids = {txn.get('transaction_id') for txn in transactions}
        for risk in risks:
            if risk.get('transaction_id') not in transaction_ids:
                issues.append(f"Foreign key mismatch: risk.transaction_id not found in transactions ({risk.get('transaction_id')})")

        # 5. Auth Log ↔ Customer
        for log in auth_logs:
            if log.get('customer_id') != customer.get('customer_id'):
                issues.append("Foreign key mismatch: auth_log.customer_id != customer.customer_id")


        return issues
    def insert_valid_data_to_db(self, record): 
        try:
            conn = psycopg2.connect(
                host='postgres',
                port=5432,
                dbname='airflow',
                user='airflow',
                password='airflow'
            )
            cursor = conn.cursor()
            # Extract data from record
            customer = record['customer']
            account = record['account']
            device = record['device']
            transactions = record['transactions']
            risks = record['risks']
            auth_logs = record['auth_logs']

            # Insert customer
            cursor.execute("""
                INSERT INTO customers (customer_id, full_name, year_of_birth, phone, email)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING;
            """, (
                customer.get('customer_id'), customer.get('full_name'), customer.get('year_of_birth'),
                customer.get('phone'), customer.get('email')
            ))

            # Insert account
            cursor.execute("""
                INSERT INTO accounts (account_id, customer_id, account_type, balance)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (account_id) DO NOTHING;
            """, (
                account.get('account_id'), customer.get('customer_id'), account.get('account_type'),
                account.get('balance')
            ))

            # Insert device
            cursor.execute("""
                INSERT INTO devices (device_id, os, customer_id, verified)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id) DO NOTHING;
            """, (
                device.get('device_id'), device.get('os'), customer.get('customer_id'), device.get('verified')
            ))

            # Insert transactions
            for txn in transactions:
                cursor.execute("""
                    INSERT INTO transactions (transaction_id, account_id, device_id, receiver_id, transaction_type, transaction_log, amount, transaction_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING;
                """, (
                    txn.get('transaction_id'), txn.get('account_id'
                    ), txn['device_id'],
                    txn.get('receiver_id'), txn.get('transaction_type'), txn.get('transaction_log'),
                    txn.get('amount'), txn.get('transaction_date')
                ))

            # Insert risks
            for risk in risks:
                cursor.execute("""
                    INSERT INTO transaction_risks (risk_id, transaction_id, risk_score, risk_level, flagged)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (risk_id) DO NOTHING;
                """, (
                    str(uuid.uuid4()),
                    risk.get('transaction_id'), risk.get('risk_score'),
                    risk.get('risk_level'), risk.get('flagged', False)
                ))

            # Insert auth logs
            for log in auth_logs:
                cursor.execute("""
                    INSERT INTO authentication_logs (log_id, otp, customer_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (log_id) DO NOTHING;
                """, (
                    log.get('log_id'), log.get('otp'), log.get('customer_id')
                ))
        except Exception as e:
            logger.error(f"Database insertion error: {e}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()
