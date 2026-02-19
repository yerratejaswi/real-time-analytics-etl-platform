from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG('real_time_etl_pipeline',
         schedule_interval='@hourly',
         default_args=default_args,
         catchup=False) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_etl',
        bash_command='python spark-etl/spark_job.py'
    )
