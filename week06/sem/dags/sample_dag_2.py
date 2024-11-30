import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'owner': 'mgcrp',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 20),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

names = ['a', 'b', 'c', 'd']
operators = []

with DAG("mgcrp_sample_dag_sem",
         default_args=DEFAULT_ARGS,
         catchup=False,
         schedule_interval="*/5 * * * *",
         max_active_runs=1,
         concurrency=1) as dag:
    
    for i in range(len(names)):
        operators.append(
            DummyOperator(task_id=f"task_{names[i]}", dag=dag)
        )
        if i > 0:
            operators[i].set_downstream(operators[i-1])
