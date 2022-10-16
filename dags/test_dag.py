from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
import datetime


dag = DAG(
    dag_id='test_dag',
    start_date=datetime.datetime(2022, 10, 12),
    schedule_interval='00 * * 1 *',
)

dag_start = DummyOperator(task_id='etl_start')

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="CREATE TABLE IF NOT EXISTS test2 (column1 varchar, column2 INT);"
)

dag_start >> create_table