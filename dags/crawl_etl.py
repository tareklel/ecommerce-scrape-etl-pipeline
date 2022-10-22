from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
import datetime
from scripts import queries
from scripts.data_quality import DataQualityOperator


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

quality_test = [
    {'test_sql':'select count(*) from factOunass',
     'expected_result':0,
     'comparison':'>'},
    {'test_sql':'select count(*) from factFarfetch',
     'expected_result':0,
     'comparison':'>'},
    {'test_sql':'select count(*) from obtbrandpricing',
     'expected_result':0,
     'comparison':'>'},
    {'test_sql':'select count(*) from factOunass where ounass_product_id is null',
     'expected_result':'=',
     'comparison':0,},
    {'test_sql':'select count(*) from factFarfetch where farfetch_product_id is null',
     'expected_result':'=',
     'comparison':0,}
]

run_quality_checks = DataQualityOperator(
    redshift_conn_id='redshift',
    checks=quality_test,
    task_id='Run_data_quality_checks',
    dag=dag
)

dag_start >> truncate_tables >> [insert_ounass, insert_farfetch] >> load_obt >> run_quality_checks
