# ----------------- ИМПОРТЫ ----------------

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
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

sql_sensor_1 = '''
    SELECT 1
    FROM business.purchases
    WHERE 1=1
        AND purchase_date >= '{calc_date}'
    LIMIT 1;
'''

# ----------------- ФУНКЦИИ ----------------

def send_msg_to_telegram(channel_id, text):
    # ----------------- ИМПОРТЫ ----------------

    import requests

    # ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

    token = BaseHook.get_connection('telegram_bot_token').password
    url = BaseHook.get_connection('telegram_bot_token').host
    url += token
    method = url + "/sendMessage"

    # ------------------- КОД ------------------

    r = requests.post(method, params={'parse_mode': 'markdown'}, data={
        "chat_id": channel_id,
        "text": text
    })

    logging.warning(f'Status code: {r.status_code}')
    logging.warning(f'Text: {r.text}')


def task_fail_tg_alert(context):
    # ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

    channels = "-4085444246"
    tg_msg_raw = """🚔🚔🚔
*Task*: `{task_id}` * [FAILED] *
*Dag*: `{dag_id}`
*Owner*: `{mention}`
*Log URL*: {exception}"""

    # ------------------- КОД ------------------

    log_url = context.get("task_instance").log_url
    exception_text_short = "[Читать далее...]({}) - {}".format(log_url)
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    dag_owner = context.get("dag").owner

    mention = '@' + dag_owner

    tg_msg = tg_msg_raw.format(
        task_id=task_id,
        dag_id=dag_id,
        owner=dag_owner,
        mention=mention,
        exception=exception_text_short
    )

    try:
        if len(str(context.get('exception'))) < 100:
            tg_msg += '\n**Exception**: {exception_text}'.format(exception_text=str(context.get('exception')))
    except:
        pass

    post_alert = PythonOperator(
        task_id="task_failure",
        python_callable=send_msg_to_telegram,
        op_kwargs={
            "channel_id": channels,
            "text": tg_msg,
        }
    )

    post_alert.execute(context)


# ------------------- КОД ------------------


DEFAULT_ARGS = {
    'owner': 'mgcrp',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=120),
    'on_failure_callback': task_fail_tg_alert,
}

with DAG("mgcrp__demo_python_fail",
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

    # Check if there is a purchase with a given date
    sensor = SqlSensor(
        task_id="waiting_for_data",
        conn_id="postgres_master",
        sql=sql_sensor_1.format(calc_date='{{ execution_date.strftime("%Y%m%d") }}'),
        timeout=7200,
        poke_interval=600,
        mode='reschedule'
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

    task_fail = PythonOperator(
        python_callable=lambda x: 1 / 0,
        dag=dag,
        task_id='fail',
    )

    # 4 - Run python
    task4 = PythonOperator(
        python_callable=send_msg_to_telegram,
        dag=dag,
        task_id='send_telegram',
        op_kwargs={
            'channel_id': '-4085444246',
            'text': 'Процесс `{{ dag.dag_id }}` за дату {{ execution_date.strftime("%Y%m%d") }} отработал!'
        }
    )

    task1 >> task_fail >> sensor >> task2 >> task3 >> task4