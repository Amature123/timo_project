from itertools import count
import json
import logging
from datetime import datetime
from collections import defaultdict
import uuid
from confluent_kafka import Consumer
import psycopg2
import time
# Setup logger
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Log file handler for monitoring auditor
file_handler = logging.FileHandler('/opt/airflow/report_logs/monitoring_auditor.log')
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)


class MonitoringAuditor:

    def __init__(self, topic='generated_customers', bootstrap_servers='kafka:9092'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        group_id = "TimoDataQualityChecker"
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'latest',
            'group.id': group_id,
        })
        self.consumer.subscribe([self.topic])
        self.violations = []
        self.STRONG_AUTH_METHODS = {'otp', 'biometric'}
        self.DAILY_LIMIT = 20_000_000
        self.TXN_LIMIT = 10_000_000
        self.VALID_TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer', 'payment']
        self.seen_transaction_ids = set()



    def run_audit(self, max_messages=20):
        try:
            time.sleep(10) #wait too let data quality checker run fisrst
            logger.info("Starting monitoring audit...")
            counter = 0
            while counter < max_messages:
                msg = self.consumer.poll(1) # Poll for new messages
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Processing customer: {data.get('customer', {}).get('customer_id')}")
                    issues = self.process_record(data)
                    if issues:
                        logger.warning(f"Violations found for customer {data.get('customer', {}).get('customer_id')}: {issues}")
                        self.violations.append({
                            "timestamp": datetime.now().isoformat(),
                            "customer_id": data.get('customer', {}).get('customer_id'),
                            "issues": issues
                        })
                        self.report()
                    else:
                        logger.info("No violations found for this record.")
                        self.insert_valid_data_to_db(data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                finally:
                   counter += 1

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
        finally:
            self.consumer.close()

    def process_record(self, data):
        customer = data.get('customer', {})
        transactions = data.get('transactions', [])
        auth_logs = data.get('auth_logs', [])
        device = data.get('device', {})
        customer_id = customer.get('customer_id')
        issue = []
        daily_sums = defaultdict(float)
        for txn in transactions:
            txn_id = txn['transaction_id']
            amount = txn['amount']
            ts = txn['transaction_date'].split('T')[0]
            for field in ['transaction_id', 'amount', 'transaction_type','receiver_id', 'transaction_date']:
                if txn.get(field) in [None, '']:
                    issue.append(f"Missing or null field: {field}")
            if txn.get('transaction_type') not in self.VALID_TRANSACTION_TYPES:
                issue.append(f"Invalid transaction_type: {txn.get('transaction_type')}")
            if not isinstance(txn.get('amount'), (int, float)) or txn.get('amount') <= 0:
                issue.append("Invalid or missing amount")
            txid = txn.get('transaction_id')
            if txid in self.seen_transaction_ids:
                issue.append(f"Duplicate transaction_id: {txid}")
            else:
                self.seen_transaction_ids.add(txid)
            daily_sums[ts] += amount

            # Get auth logs for this transaction
            txn_auth_logs = [log for log in auth_logs if log['transaction_id'] == txn_id]
            methods_used = {log['auth_method'].lower() for log in txn_auth_logs}
            # Rule 1: High-value txn must use strong auth
            if amount > self.TXN_LIMIT:
                if not methods_used.intersection(self.STRONG_AUTH_METHODS):
                    issue.append(f"High-value transaction ({amount}) without strong auth")
                    logger.warning(f"Violation found for customer {customer_id} on txn {txn_id}: High-value transaction without strong auth")



            # Rule 2: Untrusted device
            if not self.is_device_trusted(device):
                issue.append(f"Transaction from untrusted device")


        # Rule 3: Daily total over limit must use strong auth at least once that day
        for ts, total in daily_sums.items():
            if total > self.DAILY_LIMIT:
                daily_auth_methods = {log['auth_method'].lower() for log in auth_logs if log['auth_timestamp'].startswith(ts)}
                if not daily_auth_methods.intersection(self.STRONG_AUTH_METHODS):
                    issue.append(f"Daily total {total} exceeds limit without strong auth on {ts}")
                    logger.warning(f"Violation found for customer {customer_id} on date {ts}: Daily total exceeds limit without strong auth")
        return issue

    def is_device_trusted(self, device):
        # Assume device is trusted if explicitly verified
        return device.get('verified', False)

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
            transactions = record['transactions']
            risks = record['risks']
            auth_logs = record['auth_logs']

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
                    INSERT INTO authentication_logs (log_id, auth_method, transaction_id, auth_timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (log_id) DO NOTHING;
                """, (
                    log.get('log_id'), log.get('auth_method'), log.get('transaction_id'), log.get('auth_timestamp')
                ))
        except Exception as e:
            logger.error(f"Database insertion error: {e}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Data successfully inserted into the database.")

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

            report_file = '/opt/airflow/report_logs/monitoring_auditor_report.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=4)

            logger.info(f"Audit report saved to {report_file}")
        except Exception as e:
            logger.error(f"Error generating report: {e}")