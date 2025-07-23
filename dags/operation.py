from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from scripts.generate_data import KafkaUserDataProducer
from scripts.data_quality_standard import DataQualityChecker
from airflow.operators.python import PythonOperator
default_args = {
    'owner': 'timo_datam',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
def run_data_quality_checker():
    checker = DataQualityChecker()
    checker.check_data_quality()

def run_kafka_data_producer():
    producer = KafkaUserDataProducer()
    producer.send_messages()

with DAG(
    dag_id='kafka_data_generation_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  
    catchup=False,
    tags=['kafka', 'data-generator']
) as dag:
    generate_data = PythonOperator(
    task_id='generate_kafka_data',
    python_callable=run_kafka_data_producer,
)

    data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=run_data_quality_checker,
)
    generate_data >> data_quality_check
