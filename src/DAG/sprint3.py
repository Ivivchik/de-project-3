import time
import requests
import json
import logging

from requests.exceptions import RequestException
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
task_logger = logging.getLogger('project.sprint3')
postgres_conn_id = 'postgresql_de'

nickname = 'iv-ivchik'
cohort = '2'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    task_logger.info('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')


def get_report(ti):
    task_logger.info('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        task_logger.info(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError(f"Error while attemp to get report_id, task_id: {task_id} not complited")

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')


def get_increment(date, ti):
    task_logger.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    task_logger.info(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise RequestException(f'Error while attemt to get increment_id, increment_id is: {increment_id}')
    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'increment_id={increment_id}')


def get_s3_filename(filename, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    response = requests.get(s3_filename)
    if not response:
        raise RequestException(f'Error while attemt to get response, response is: {response}')
    ti.xcom_push(key='s3_filename', value=s3_filename)
    task_logger.info(f's3_filename={s3_filename}')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        max_active_runs=1,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=2),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    get_s3_filename = PythonOperator(
        task_id='get_s3_filename',
        python_callable=get_s3_filename,
        op_kwargs={'filename': 'user_orders_log_inc.csv'})

    upload_user_order_inc_from_s3 = PostgresOperator(
        task_id='upload_user_order_inc_from_s3',
        postgres_conn_id=postgres_conn_id,
        sql="COPY {{ params.pg_schema }}.{{ params.pg_table }} FROM PROGRAM 'curl {{ ti.xcom_pull(key='s3_filename') }}' HEADER CSV DELIMITER ',';",
        params={'pg_schema': 'staging', 'pg_table': 'user_order_log_inc'}
    )

    update_user_order_log = PostgresOperator(
        task_id='update_user_order_log',
        postgres_conn_id=postgres_conn_id,
        sql='sql/insert_user_order_log.sql'
    )

    truncate_user_order_inc = PostgresOperator(
        task_id='truncate_user_order_inc',
        postgres_conn_id=postgres_conn_id,
        sql='TRUNCATE TABLE {{ params.pg_schema }}.{{ params.pg_table }}',
        params={'pg_schema': 'staging', 'pg_table': 'user_order_log_inc'}
    )
                   
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        params={"date": {business_dt}}
    )
    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        params={"date": {business_dt}}
    )

    create_mart_f_customer_retention = PostgresOperator(
        task_id='create_mart_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/create_mart_f_customer_retention.sql",
    )

    create_staging_user_order_log_inc = PostgresOperator(
        task_id='create_staging_user_order_log_inc',
        postgres_conn_id=postgres_conn_id,
        sql="sql/create_staging_user_order_log_inc.sql",
    )

    dummy = DummyOperator(
        task_id = 'dummy',
    )

    (
            generate_report
            >> get_report
            >> get_increment
            >> get_s3_filename
            >> [create_staging_user_order_log_inc, create_mart_f_customer_retention]
            >> upload_user_order_inc_from_s3
            >> update_user_order_log
            >> truncate_user_order_inc
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> dummy
            >> [update_f_sales, update_f_customer_retention]
    )

