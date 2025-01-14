from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl import transform, check_and_extract_data, load_data


# Default arguments
default_args = {
    'owner': 'nasir',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Define DAG
dag = DAG(
    'airflow_dag_amazon',
    default_args=default_args,
    description='Our DAG',
    schedule_interval=None #timedelta(days=1),
)


# Define tasks
extraction = PythonOperator(
    task_id='extract',
    python_callable=check_and_extract_data,
    dag=dag,
)

transformation = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)
loading = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)


# Task dependencies
extraction >> transformation >> loading
