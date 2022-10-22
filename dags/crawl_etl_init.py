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
    dag_id='set_up_redshift',
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
        JSON '{}'
        region 'us-east-1'
    """.format(
        'ounass_staging', 
        config.ounass_path,
        credentials.access_key,
        credentials.secret_key,
        config.ounass_json_paths
    )
)

stage_farfetch_to_redshift = PostgresOperator(
    task_id='stage_farfetch_to_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        region 'us-east-1'
    """.format(
        'farfetch_staging', 
        config.farfetch_path, 
        credentials.access_key,
        credentials.secret_key,
        config.farfetch_json_paths
    )
)

brand_to_redshift = PostgresOperator(
    task_id='brand_to_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        region 'us-east-1'
    """.format(
        'dimBrand', 
        config.brand_path, 
        credentials.access_key,
        credentials.secret_key,
    )
)

category_to_redshift = PostgresOperator(
    task_id='category_to_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        region 'us-east-1'
    """.format(
        'dimCategory', 
        config.brand_path, 
        credentials.access_key,
        credentials.secret_key,
    )
)

gender_to_redshift = PostgresOperator(
    task_id='gender_to_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        region 'us-east-1'
    """.format(
        'dimGender', 
        config.gender_path, 
        credentials.access_key,
        credentials.secret_key,
    )
)


dag_start >> drop_tables >> create_table >> [
    brand_to_redshift, 
    category_to_redshift, 
    gender_to_redshift,
    stage_farfetch_to_redshift, 
    stage_ounass_to_redshift 
    ]
