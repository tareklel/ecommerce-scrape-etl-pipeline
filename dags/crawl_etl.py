from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime
from scripts import queries


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

insert_ounass = PostgresOperator(
    task_id="insert_ounass",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.load_factOunass
)

insert_farfetch = PostgresOperator(
    task_id="insert_farfetch",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.load_factFarfetch
)

load_obt = PostgresOperator(
    task_id="load_obt",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.load_obt
)

dag_start >> truncate_tables >> [insert_ounass, insert_farfetch] >> load_obt
