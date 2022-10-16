from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
import datetime
from scripts import queries


aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()


dag = DAG(
    dag_id='test_dag',
    start_date=datetime.datetime(2022, 10, 12),
    schedule_interval='00 * * 1 *',
)

dag_start = DummyOperator(task_id='etl_start')

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.drop_tables
)

create_table = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_all_tables
)

stage_ounass_to_redshift = PostgresOperator(
    task_id='stage_ounass_to_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 's3://scraper-bucket-lux/category_jsonpath.json'
        region 'us-east-1'
    """.format('ounass_staging', 
    's3://scraper-bucket-lux/ounass_mini.json', 
    credentials.access_key,
    credentials.secret_key
    )
)

dag_start >> drop_tables >> create_table
