import json
import logging
from datetime import datetime
from collections import defaultdict
import uuid
from confluent_kafka import Consumer
# Setup logger
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class MonitoringAuditor:


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
        self.violations = []
        self.STRONG_AUTH_METHODS = ['biometric', 'otp']
        self.DAILY_LIMIT = 20_000_000
        self.TXN_LIMIT = 10_000_000

    def run_audit(self, max_messages=50):
            try:
                count = 0
                while count < max_messages:
                    msg = self.consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                    count += 1
                    data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Processing record: {data.get('customer', {}).get('customer_id')}")
                    self.process_record(data)

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

        # --- Auth history per day
        auth_by_date = defaultdict(set)
        for auth in auth_logs:
            ts = auth.get('timestamp')
            method = auth.get('method')
            if ts and method:
                date_str = ts.split("T")[0]
                auth_by_date[date_str].add(method)

        # --- Transaction grouping by date and sum
        daily_sums = defaultdict(float)
        for txn in transactions:
            date_str = txn['transaction_date'].split("T")[0]
            amount = txn['amount']
            daily_sums[date_str] += amount

            # Rule 1: Transaction > 10M VND → must have strong auth that day
            if amount > self.TXN_LIMIT:
                if not self.has_strong_auth(auth_by_date[date_str]):
                    self.violations.append({
                        "customer_id": customer_id,
                        "txn_id": txn['transaction_id'],
                        "issue": f"High-value transaction ({amount}) without strong auth on {date_str}"
                    })

            # Rule 2: New/untrusted device
            if not self.is_device_trusted(device):
                self.violations.append({
                    "customer_id": customer_id,
                    "txn_id": txn['transaction_id'],
                    "issue": "Transaction from untrusted device"
                })

        # Rule 3: Daily total > 20M VND → must have strong auth
        for date_str, total in daily_sums.items():
            if total > self.DAILY_LIMIT:
                if not self.has_strong_auth(auth_by_date[date_str]):
                    self.violations.append({
                        "customer_id": customer_id,
                        "date": date_str,
                        "issue": f"Daily total {total} exceeds limit without strong auth"
                    })

    def has_strong_auth(self, methods_set):
        return any(m in methods_set for m in self.STRONG_AUTH_METHODS)

    def is_device_trusted(self, device):
        # Placeholder rule — update with real trust check logic
        trusted_os = ['iOS', 'Android']
        return device.get('os') in trusted_os and 'device_id' in device

    def report(self, output_file=None):
            if not self.violations:
                logger.info("No risk-based compliance issues found.")
            else:
                for v in self.violations:
                    logger.warning(f"Risk audit violation: {v}")

            if output_file:
                with open(output_file, "w") as f:
                    json.dump(self.violations, f, indent=2)

            return self.violations
