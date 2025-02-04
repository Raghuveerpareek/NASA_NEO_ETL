from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  
from airflow.utils.dates import days_ago
from datetime import datetime
from NASA_NEO_ETL_V2 import run_nasa_etl


with DAG(
    default_args={
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'end_date': datetime(2025, 2, 5),
    'execution_timeout': timedelta(seconds=300),
    },
    description="Nasa_Neo_Project_DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 2, 4),
    catchup=False,
) as dag:
    run_etl = PythonOperator(
        task_id="complete_neo_etl",
        python_callable= run_nasa_etl,
        dag=dag
    )

run_etl