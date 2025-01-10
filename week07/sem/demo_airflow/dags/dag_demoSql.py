# ----------------- ИМПОРТЫ ----------------

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

sql_script_1 = '''
    CREATE SCHEMA IF NOT EXISTS airflow;
'''

sql_script_2 = '''
    DROP TABLE IF EXISTS airflow.gmv_{calc_date};
'''

sql_script_3 = '''
    CREATE TABLE airflow.gmv_{calc_date} AS
    SELECT
        pu.store_id,
        pr.category_id,
        SUM(pi.product_price * pi.product_count) AS sales_sum
    FROM
        business.purchase_items pi
    JOIN
        business.products pr ON pi.product_id = pr.product_id
    JOIN
        business.purchases pu ON pi.purchase_id = pu.purchase_id
    GROUP BY
        pu.store_id,
        pr.category_id
    ;
'''

DEFAULT_ARGS = {
    'owner': 'mgcrp',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120)
}

# ----------------- ФУНКЦИИ ----------------

pass

# ------------------- КОД ------------------

with DAG("mgcrp__demo_sql",
         default_args=DEFAULT_ARGS,
         catchup=False,
         schedule_interval="*/5 * * * *",
         max_active_runs=1,
         concurrency=1) as dag:
    
    # 1 - Create schema
    task1 = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_master",
        sql=sql_script_1,
    )

    # 2 - drop table
    task2 = PostgresOperator(
        task_id="drop_table",
        postgres_conn_id="postgres_master",
        sql=sql_script_2.format(calc_date='{{ execution_date.strftime("%Y%m%d") }}'),
    )
    
    # 3 - create table
    task3 = PostgresOperator(
        task_id="perform_etl",
        postgres_conn_id="postgres_master",
        sql=sql_script_3.format(calc_date='{{ execution_date.strftime("%Y%m%d") }}'),
    )

    task1 >> task2 >> task3