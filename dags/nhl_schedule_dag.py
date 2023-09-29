import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator as ECSOperator
from datetime import datetime, timedelta
import pendulum


# Environment Variable
from airflow.models import Variable

local_tz = pendulum.timezone('America/Denver')
subnet = 'subnet-086f7f66dfd373f93'
logs_group = 'airflow-nhl-pipeline-airflow-env-Task'
cluster = 'nhl-data-pipeline'
task_definition = 'airflow-data-pipelines:5'
container_name = 'airflow-data-pipelines'

# Script variables
source = 'seasons'
endpoint = 'https://www.hockey-reference.com/leagues/'
year = datetime.year
s3_bucket_name = 'nhl-data-raw'
snowflake_conn = 'standard'
env = 'production'


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
    nhl_seasons = ECSOperator(
        task_id="nhl_seasons",
        cluster=f"{cluster}",
        task_definition=f"{task_definition}",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": f"{container_name}",
                    "command": [
                        f"python3 src/scripts/snowflake_transfer.py {source} {endpoint} {year} {s3_bucket_name} {snowflake_conn} {env}"]
                }
            ],
        },
        network_configuration={'awsvpcConfiguration': {'assignPublicIp': 'ENABLED', 'subnets': [f'{subnet}']}},
        awslogs_group=f"{logs_group}"
    )
