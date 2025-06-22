from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'pyspark_dbt_dag',
    default_args=default_args,
    description='Run PySpark to read from PostgreSQL, write to S3, and dbt to transform to CSV',
    schedule=None,
    start_date=datetime(2025, 6, 20),
    catchup=False,
) as dag:

    run_pyspark_job = PythonOperator(
        task_id='run_pyspark_job',
        python_callable=lambda: os.system('/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/airflow/dags/read_postgres.py'),
    )
    print("Inside DAG" )
    
    run_pyspark_job
    print("Inside------DAG" )
"""
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_project',
    )
"""
    