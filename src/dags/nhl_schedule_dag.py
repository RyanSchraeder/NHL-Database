import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from snowflake_transfer import SnowflakeIngest
from snowflake_queries import (
    snowflake_stages,
    snowflake_cleanup,
    snowflake_schema,
    snowflake_ingestion
)

# Environment Variable
from airflow.models import Variable

local_tz = pendulum.timezone('America/Denver')
subnet = 'subnet-086f7f66dfd373f93'
logs_group = 'airflow-Test-Task'

# Set up class and ingest data from Raw Source to S3
execute = SnowflakeIngest()


@task(task_id="nhl_extract_transform")
def nhl_extract_transform(source, endpoint, year, s3_bucket_name, snowflake_conn):

    # Set up the module
    run = execute(source, endpoint, year, s3_bucket_name, snowflake_conn)

    # Extract raw data and transform
    output_df = run.file_parser()

    # Load transformed data to S3
    run.s3_parser(output_df)


@task(task_id="snowflake_setup")
def snowflake_setup(source, endpoint, year, s3_bucket_name, snowflake_conn):

    # Set up the module
    run = execute(source, endpoint, year, s3_bucket_name, snowflake_conn)

    # CREATE STAGES FROM S3 -> SNOWFLAKE
    run.snowflake_query_exec(queries=snowflake_stages)

    # CREATE SCHEMA
    run.snowflake_query_exec(queries=snowflake_schema())


@task(task_id="snowflake_ingest")
def snowflake_ingest(source, endpoint, year, s3_bucket_name, snowflake_conn):

    # Set up the module
    run = execute(source, endpoint, year, s3_bucket_name, snowflake_conn)

    # DEDUPE DATA
    run.snowflake_query_exec(queries=snowflake_cleanup(year))

    # INGEST DATA FROM S3
    run.snowflake_query_exec(queries=snowflake_ingestion())


with DAG(
    'nhl_season_schedule_dag',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description="ADS 00725 STB ADS DAG. Schedule: 4:30PM MST Everyday after Scope STB Tuner Platform OPS builds.",
    schedule_interval='30 16 * * *',
    start_date=datetime.now(),
    catchup=False
) as dag:
    nhl_extract_transform = PythonOperator(
        task_id="nhl_extract_transform",
        launch_type="FARGATE",
        network_configuration={'awsvpcConfiguration': {'assignPublicIp': 'ENABLED', 'subnets': [f'{subnet}']}},
        awslogs_group=f"{logs_group}",
        python_callable=nhl_extract_transform,
        op_kwargs={
            "source": "seasons",
            "endpoint": "https://www.hockey-reference.com/leagues/",
            "year": datetime.now().year,
            "s3_bucket_name": "nhl-data-raw",
            "snowflake_conn": "standard"
        }
    )

    snowflake_setup = PythonOperator(
        task_id="snowflake_setup",
        launch_type="FARGATE",
        network_configuration={'awsvpcConfiguration': {'assignPublicIp': 'ENABLED', 'subnets': [f'{subnet}']}},
        awslogs_group=f"{logs_group}",
        python_callable=snowflake_setup,
        op_kwargs={
            "source": "seasons",
            "endpoint": "https://www.hockey-reference.com/leagues/",
            "year": datetime.now().year,
            "s3_bucket_name": "nhl-data-raw",
            "snowflake_conn": "standard"
        }
    )

    snowflake_ingest = PythonOperator(
        task_id="snowflake_setup",
        launch_type="FARGATE",
        network_configuration={'awsvpcConfiguration': {'assignPublicIp': 'ENABLED', 'subnets': [f'{subnet}']}},
        awslogs_group=f"{logs_group}",
        python_callable=snowflake_ingest,
        op_kwargs={
            "source": "seasons",
            "endpoint": "https://www.hockey-reference.com/leagues/",
            "year": datetime.now().year,
            "s3_bucket_name": "nhl-data-raw",
            "snowflake_conn": "standard"
        }
    )

    # Dependencies
    nhl_extract_transform >> snowflake_setup >> snowflake_ingest