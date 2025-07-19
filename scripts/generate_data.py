import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

NUM_CUSTOMERS = 10
NUM_ACCOUNTS_PER_CUSTOMER = (1, 3)
NUM_TRANSACTIONS_PER_ACCOUNT = (5, 15)

faker = Faker()


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_customers(cur):
    customers = []
    for _ in range(NUM_CUSTOMERS):
        full_name = faker.name()
        phone = faker.unique.phone_number()
        email = faker.unique.email()
        cur.execute("""
            INSERT INTO Customers (full_name, phone, email)
            VALUES (%s, %s, %s)
            RETURNING customer_id;
        """, (full_name, phone, email))
        customers.append(cur.fetchone()[0])
    return customers

def insert_accounts(cur, customer_ids):
    account_ids = []
    for customer_id in customer_ids:
        for _ in range(random.randint(*NUM_ACCOUNTS_PER_CUSTOMER)):
            cur.execute("""
                INSERT INTO account (customer_id)
                VALUES (%s)
                RETURNING account_id;
            """, (customer_id,))
            account_ids.append((cur.fetchone()[0], customer_id))
    return account_ids

def insert_transactions(cur, account_pairs):
    transaction_ids = []
    for account_id, _ in account_pairs:
        for _ in range(random.randint(*NUM_TRANSACTIONS_PER_ACCOUNT)):
            txn_type = random.choice(['deposit', 'withdrawal'])
            amount = random.choice([50, 100, 200, 5000, 10000, 20000])  
            txn_date = faker.date_time_between(start_date='-1y', end_date='now')
            cur.execute("""
                INSERT INTO Transactions (account_id, transaction_type, amount, transaction_date)
                VALUES (%s, %s, %s, %s)
                RETURNING transaction_id;
            """, (account_id, txn_type, amount, txn_date))
            transaction_ids.append(cur.fetchone()[0])
    return transaction_ids

def insert_devices(cur, customer_ids):
    for customer_id in customer_ids:
        for _ in range(random.randint(1, 2)):
            device_name = faker.word().capitalize() + " " + str(random.randint(100, 999))
            os = random.choice(["Android", "iOS", "Windows", "Linux", "macOS"])
            cur.execute("""
                INSERT INTO devices (device_name, os, user_id)
                VALUES (%s, %s, %s);
            """, (device_name, os, customer_id))

def insert_auth_logs(cur, customer_ids):
    for customer_id in customer_ids:
        for _ in range(random.randint(1, 3)):
            otp = str(random.randint(100000, 999999))
            login_time = faker.date_time_between(start_date='-1y', end_date='now')
            device_name = faker.word().capitalize() + " " + str(random.randint(100, 999))
            cur.execute("""
                INSERT INTO authentication_log (otp, user_id, login_time, device_name)
                VALUES (%s, %s, %s, %s);
            """, (otp, customer_id, login_time, device_name))

def insert_transaction_risks(cur, transaction_ids):
    for txn_id in random.sample(transaction_ids, k=int(len(transaction_ids) * 0.4)):  # flag 40%
        score = random.randint(0, 100)
        level = 'low' if score < 33 else 'medium' if score < 66 else 'high'
        reason = faker.sentence()
        flagged = score > 70
        evaluated = faker.date_time_between(start_date='-6mo', end_date='now')
        cur.execute("""
            INSERT INTO transaction_risks (transaction_id, risk_score, risk_level, reason, flagged, evaluated_at)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (txn_id, score, level, reason, flagged, evaluated))

# ----- Run Everything -----
def main():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                print("Inserting customers...")
                customers = insert_customers(cur)

                print("Inserting accounts...")
                accounts = insert_accounts(cur, customers)

                print("Inserting transactions...")
                transactions = insert_transactions(cur, accounts)

                print("Inserting devices...")
                insert_devices(cur, customers)

                print("Inserting authentication logs...")
                insert_auth_logs(cur, customers)

                print("Inserting transaction risks...")
                insert_transaction_risks(cur, transactions)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
