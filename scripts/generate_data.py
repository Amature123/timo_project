import random
import json
from datetime import datetime

from faker import Faker
from confluent_kafka import Producer
import logging
import uuid

# Logging Configuration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class KafkaUserDataProducer:
    def __init__(self, bootstrap_servers='kafka:9092', topic='generated_customers'):
        self.fake = Faker('vi_VN')
        self.topic = topic
        self.config = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.config)
        self.fake.unique.clear()

    def _json_serializer(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def _generate_fake_user(self):
        customer_id = random.randint(10**11, 10**12 - 1)
        account_id = random.randint(10**15, 10**16 - 1)
        os_list = ['Android', 'iOS', 'Windows', 'Linux']

        customer = {
            "customer_id": customer_id,
            "full_name": self.fake.name(),
            "year_of_birth": self.fake.date_of_birth(minimum_age=18, maximum_age=70).year,
            "phone": self.fake.unique.phone_number(),
            "email": self.fake.unique.email(),
            "nation": self.fake.country()
        }

        account = {
            "account_id": account_id,
            "customer_id": customer_id,
            "account_type": random.choice(['savings', 'personal', 'business']),
            "created_at": self.fake.date_time_this_year().isoformat(),
            "balance": round(random.uniform(100000, 50000000), 2)
        }

        device = {
            "device_id": self.fake.uuid4(),
            "customer_id": customer_id,
            "os": random.choice(os_list),
            "verified": random.choice([True, False])
        }

        transactions = []
        risks = []
        for _ in range(2):
            amount = round(random.uniform(50000, 100000000), 2)
            transaction_id = self.fake.uuid4()
            txn_type = random.choice(['deposit', 'withdrawal','transfer'])
            transaction_date = self.fake.date_time_this_year()
            if txn_type == 'transfer':
                receiver_id = random.randint(10**11, 10**12 - 1)
            if txn_type == 'withdrawal':
                receiver_id = 666 # Special case for withdrawal
            else:
                receiver_id = 333 # Special case for deposit 
            transactions.append({
                "transaction_id": transaction_id,
                "account_id": account_id,
                "device_id": device["device_id"],
                "transaction_type": txn_type,
                "amount": amount,
                "transaction_date": transaction_date.isoformat(),
                "receiver_id": receiver_id,
                "transaction_log": f"Transaction {txn_type} of {amount} VND on {transaction_date.strftime('%Y-%m-%d %H:%M:%S')}"
            })

            if amount > 50000000:
                risk = {"risk_score": random.randint(80, 100), "risk_level": "high", "flagged": True,
                        "reason": "High-value transaction"}
            elif amount > 10000000:
                risk = {"risk_score": random.randint(40, 79), "risk_level": "medium",
                        "flagged": random.choice([True, False]), "reason": "Moderate-value transaction"}
            else:
                risk = {"risk_score": random.randint(0, 39), "risk_level": "low", "flagged": False,
                        "reason": "Low-value transaction"}

            risk["transaction_id"] = transaction_id
            risks.append(risk)

        auth_logs = []
        for _ in range(random.randint(1, 2)):
            auth_logs.append({
                "log_id": self.fake.uuid4(),
                "otp": f"{random.randint(0, 999999):06}",
                "customer_id": customer_id,
            })

        return {
            "customer": customer,
            "account": account,
            "device": device,
            "transactions": transactions,
            "risks": risks,
            "auth_logs": auth_logs
        }

    def send_messages(self, n_customers=50):
        try:
            for _ in range(n_customers):
                payload = self._generate_fake_user()
                logger.info(f"Sending customer {payload['customer']['customer_id']} to Kafka...")
                
                self.producer.produce(
                    self.topic,
                    value=json.dumps(payload, default=self._json_serializer).encode('utf-8'),
                    callback=self._delivery_report
                )
                self.producer.poll(0)  
                logger.info(f"Produced message {payload}")

            self.producer.flush()
            logger.info("All messages sent.")
        except Exception as e:
            logger.error(f"Error sending messages to Kafka: {e}")

