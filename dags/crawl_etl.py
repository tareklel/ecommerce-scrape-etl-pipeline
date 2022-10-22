from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
import datetime
from scripts import queries
from scripts import config


aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()


dag = DAG(
    dag_id='crawl_etl',
    start_date=datetime.datetime(2022, 10, 12),
    schedule_interval='00 * * 1 *',
)

dag_start = DummyOperator(task_id='etl_start')

truncate_tables = PostgresOperator(
    task_id="truncate_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.truncate_tables
)


dag_start >> truncate_tables
