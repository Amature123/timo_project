from itertools import count
from click import group
from confluent_kafka import Consumer, KafkaError,Producer
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
# log file handler for data quality checker
file_handler = logging.FileHandler('/opt/airflow/report_logs/data_quality_checker.log')
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

class DataQualityChecker:
    def __init__(self, topic='generated_customers', bootstrap_servers='kafka:9092'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        group_id = "TimoDataQualityChecker"
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
        self.producer_topic = 'data_quality_violations'
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
        self.violations = []
    
    def _json_serializer(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    def check_data_quality(self, max_messages=20):
        try:
            count = 0
            while count < max_messages:
                msg = self.consumer.poll(0.5) 
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
                    self.violations.append({
                        "customer_id": data.get('customer', {}).get('customer_id'),
                        "issues": issues
                    })
                    self.report()
                else:
                    logger.info("Data quality check passed.")
                    self.insert_valid_data_to_db(data)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            logger.info("Data quality check completed.")
            
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
        for field in ['device_id', 'os','verified']:
            if not device.get(field):
                issues.append(f"Missing or null field: {field}")


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

        # 5. Auth Log ↔ Transaction
        for log in auth_logs:
            if log.get('transaction_id') not in transaction_ids:
                issues.append("Foreign key mismatch: auth_log.transaction_id != transaction.transaction_id")

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
                INSERT INTO devices (device_id, os, customer_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (device_id) DO NOTHING;
            """, (
                device.get('device_id'), device.get('os'), customer.get('customer_id')
            ))

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()
    
    def report(self):
        try:
            if not self.violations:
                logger.info("No violations found.")
                return

            clean_violations = []
            for v in self.violations:
                clean_v = {}
                for k, val in v.items():
                    if isinstance(val, set):
                        clean_v[k] = list(val)
                    else:
                        clean_v[k] = val
                clean_violations.append(clean_v)

            report = {
                "timestamp": datetime.now().isoformat(),
                "violations": clean_violations
            }

            report_file = '/opt/airflow/report_logs/data_quality_report.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=4)

            logger.info(f"Audit report saved to {report_file}")
        except Exception as e:
            logger.error(f"Error generating report: {e}")