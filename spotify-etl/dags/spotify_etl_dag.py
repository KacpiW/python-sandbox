from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id="spotify_dag", default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    run_spotify = PythonOperator(
        task_id="Spotify_ETL", python_callable=run_spotify_etl)

    run_spotify
