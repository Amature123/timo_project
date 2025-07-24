##  Project Overview

This project is a comprehensive ETL pipeline designed for the **Timo Data Engineer Internship** assignment. It simulates a secure, regulation-compliant digital banking platform by integrating core components of data engineering such as data generation, streaming, validation, and orchestration.

The pipeline mirrors essential operations of modern banking systems including:

* Customer identity management
* Account and transaction tracking
* Device authentication
* Risk and fraud monitoring

Built with **Apache Kafka**, **PostgreSQL**, and **Apache Airflow**, this solution emphasizes **data quality**, **compliance** with 2345/QĐ-NHNN (2023), and **system reliability**.

###  Key Features

* **Realistic Data Simulation**
  Generate Vietnamese-styled customer, account, transaction, and device data using `Faker`.

* **Kafka-Based Streaming**
  Produce and consume data using Kafka (KRaft mode, no ZooKeeper), enabling real-time flow.

* **Data Quality Validation**
  Validate incoming data with robust checks (e.g., nulls, uniqueness, identity format) before database insertion.

* **Risk Monitoring & Auditing**
  Detect suspicious transactions using rule-based risk indicators and behavioral patterns.

* **PostgreSQL Integration**
  Persist clean and validated data into a relational schema auto-initialized on startup.

* **Orchestration via Apache Airflow**
  Automate the ETL workflow with scheduled DAGs and monitor task execution via a web UI.

###  Tech Stack

* **Python**
* **Apache Kafka (KRaft mode)**
* **Apache Airflow**
* **PostgreSQL**
* **Docker & Docker Compose**

### 🗂 Project Structure

```
├── docker-compose.yml         # Define services: Kafka, Postgres, Airflow
├── sql/
│   └── schema.sql             # DB schema with constraints
├── scripts/
│   ├── generate_data.py       # Kafka Producer: simulate & send data
│   ├── data_quality_standard.py # Kafka Consumer: validate & insert data
│   ├── monitor.py             # Monitor suspicious behavior / risk
│   └── __init__.py            # Marks script folder as a module
├── dags/
│   └── operation.py           # Airflow DAG definition
├── report_logs/               # Log files and validation results
└── .env.example               # Environment config (example)
```

### 🚀 How to Run

1. **Clone this repo**

   ```bash
   git clone https://github.com/Amature123/timo_project.git
   cd timo_project
   ```

2. **Set up environment**
   Copy `.env.example` to `.env` and configure it as needed.

3. **Start services**

   ```bash
   docker compose up -d
   ```

4. **Access Airflow**
   Go to [localhost:8080](http://localhost:8080), log in, and trigger the DAG.
