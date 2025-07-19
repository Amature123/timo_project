from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from traitlets import default
from scripts.generate_data import get_connection, insert_customers, insert_accounts, insert_transactions
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

dag = DAG(
    'timo_dag',
    default_args=default_args,
    description='A DAG to generate and insert data into Timo database',
    schedule_interval='* * * * *', 
    catchup=False,
)

