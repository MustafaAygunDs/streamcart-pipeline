from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
import sys

default_args = {
    'owner': 'streamcart',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'streamcart_pipeline',
    default_args=default_args,
    description='StreamCart end-to-end data pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['streamcart', 'etl', 'ecommerce'],
)

silver_task = BashOperator(
    task_id='run_silver_transformation',
    bash_command='cd /opt/airflow/dags && python3 /opt/airflow/dags/scripts/orders_silver.py',
    dag=dag,
)

quality_task = BashOperator(
    task_id='run_data_quality_checks',
    bash_command='cd /opt/airflow/dags && python3 /opt/airflow/dags/scripts/orders_expectations.py',
    dag=dag,
)

gold_task = BashOperator(
    task_id='run_gold_transformation',
    bash_command='cd /opt/airflow/dags && python3 /opt/airflow/dags/scripts/orders_gold.py',
    dag=dag,
)

silver_task >> quality_task >> gold_task
