import pandas as pd
import argparse

import datetime as dt
import time
import sys
import os
import boto3

from src.connectors import s3_conn, get_legacy_session
from src.snowflake_queries import *
from src.preprocessing import DataTransform

from src.helpers import setup, snowflake_query_exec 

# Orchestration
from prefect import flow, task, get_run_logger

# Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##

# Data transformations
transform = DataTransform


@task(name="file_parser")
def file_parser(source, url, snowflake_conn, year: int = dt.datetime.now().year):
    """ Download raw source data and upload to S3
        Data Source: hockeyreference.com
    """
    try:
        logging = get_run_logger()
        response = get_legacy_session().get(url)
        dataframes = pd.read_html(response.text)
        dataframe = pd.concat(dataframes, axis=0, ignore_index=True).reset_index(drop=True)

        logging.info('Transforming data...')
        dataframe = transform.seasons(dataframe, year)

        logging.info('Checking column mappings...')
        checks = snowflake_query_exec(snowflake_checks('regular_season'), method=snowflake_conn)
        len_source, len_dest = len(dataframe.columns), len(checks)

        logging.info(
            f'Destination mappings: {len_dest}'
            '\n'
            f'Source mappings: {len_source}'
        )

        if not len_dest == len_source:
            logging.error('Length of source columns does not match number of destination columns.')
            sys.exit(1)

        logging.info(
            f'Retrieved data with columns: {dataframe.columns}'
            f'\n'
            f'Preview: \n{dataframe.head(3)}'
        )

        return dataframe

    except Exception as e:
        logging.error(f'An error occurred while retrieving raw data: {e}')


@task(name="s3_upload")
@s3_conn
def s3_parser(filename: str, data: pd.DataFrame, s3_folder: str, s3_bucket_name: str = 'nhl-data-raw'):
    try:

        logging = get_run_logger()

        # Establish connection
        s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

        os.path.join('src/')

        if os.path.exists('data'):
            pass
        else:
            os.mkdir('data', mode=0o777)

        # Convert DF to CSV File
        path = f'./data/{filename}.csv'
        data.to_csv(path, index=False)

        logging.info(f'Data stored at {path}')

        # Build the targets
        dst, filename = f'{s3_bucket_name}', f'{s3_folder}/{filename}.csv'

        # Retrieve S3 paths & store raw file to s3
        logging.info(f'Storing parsed data in S3 at {filename}')
        s3_client.upload_file(
            path, dst, filename
        )
        logging.info(f'Successfully uploaded to S3')

    except Exception as e:
        logging.error(f'An error occurred when storing data in S3: {e}')
        sys.exit(1)


@task(name="snowflake_base_model")
def snowflake_base_model(snowflake_conn):
    logging = get_run_logger()

    # Create stages
    logging.info("Updating snowflake stages if needed")
    snowflake_query_exec(snowflake_stages(), method=snowflake_conn)

    # Create Schemas
    logging.info("Updating table schemas if needed")
    snowflake_query_exec(snowflake_schema(), method=snowflake_conn)

    return


@task(name="snowflake_load")
def snowflake_load(table, year, source, snowflake_conn):
    logging = get_run_logger()

    # DEDUPE FROM SNOWFLAKE
    logging.info(f"Deduplicating yearly record data to refresh the schedule")
    snowflake_query_exec(snowflake_cleanup(table, year), method=snowflake_conn)

    # INGEST RAW DATA TO SNOWFLAKE
    logging.info(f"Updating yearly record data")
    snowflake_query_exec(snowflake_ingestion(table, source), method=snowflake_conn)

    return


@flow(
    name='nhl_regular_seasons', retries=1, retry_delay_seconds=5, log_prints=True
)
def nhl_snowflake_ingest(source, endpoint, year, s3_bucket_name, snowflake_conn, env):
    start = time.time()
    logging = get_run_logger()

    # Prepare Source URL
    url, filename = setup(source, endpoint, year)
    logging.info(f'Source URL: {url}, Filename: {filename}')

    # Prepare snowflake stages and schemas
    logging.info('Preparing snowflake base model for ingestion')
    snowflake_base_model(snowflake_conn)

    # Parse Files from Raw Endpoint
    # Only store data in S3 in Production
    if env == "development":
        try:
            logging.info("Extracting raw data from source, formatting and transformation, and loading it to S3")
            file_parser(source, url, snowflake_conn, year)
            logging.info(
                "\n"
                "\t Process executed successfully in development. "
                "\t No data was uploaded to S3 or Snowflake. "
                "\t To try testing out your ingestion completely, use the production branch."
                "\n"
            )
        except Exception as e:
            logging.error(
                f'Test failed while executing in development. Please review: \n'
                f'\t\t{e}'
            )
    else:
        # INGEST RAW DATA TO S3
        logging.info("Extracting raw data from source, formatting and transformation, and loading it to S3")
        output_df = file_parser(source, url, snowflake_conn, year)
        s3_parser(filename=filename, data=output_df, s3_folder=source, s3_bucket_name=s3_bucket_name)

        # MINOR TRANSFORMATION & TRANSFER RAW DATA TO SNOWFLAKE
        # logging.info("Extracting raw data from S3 to transferred into Snowflake")
        snowflake_load('regular_season', year, source, snowflake_conn)

    end = time.time() - start
    logging.info(f'Process Completed. Time elapsed: {end}')


if __name__ in "__main__":

    parser = argparse.ArgumentParser(
        prog="SnowflakeIngestion",
        description="Move data from raw S3 uploads to a produced Schema in Snowflake"
    )

    parser.add_argument('source')
    parser.add_argument('--endpoint', default='https://www.hockey-reference.com/leagues/')
    parser.add_argument('--year', default=dt.datetime.now().year)
    parser.add_argument('--s3_bucket_name', default='nhl-data-raw')
    parser.add_argument('--snowflake_conn', default='standard')
    parser.add_argument('--env', default='development')

    args = parser.parse_args()

    source = args.source if args.source is not None else ""
    endpoint = args.endpoint if args.endpoint is not None else ""
    year = args.year if args.year is not None else ""

    # Example S3 URI : s3://nhl-data-raw/season/regular_season.csv
    s3_bucket_name = args.s3_bucket_name if args.s3_bucket_name is not None else ""
    snowflake_conn = args.snowflake_conn if args.snowflake_conn is not None else ""
    env = args.env if args.env is not None else "development"

    print(f"Received Arguments: {args}")
    
    # Execute the pipeline
    nhl_regular_seasons(source, endpoint, year, s3_bucket_name, snowflake_conn, env)
