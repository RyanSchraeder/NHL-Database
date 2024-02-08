import pandas as pd
import argparse

import datetime as dt
import time
import sys
import os
import boto3

from connectors import get_snowflake_connection, s3_conn
from snowflake.connector import ProgrammingError
from connectors import get_legacy_session
from snowflake_queries import *
from preprocessing import DataTransform

# Orchestration
from prefect import flow, task, get_run_logger

# Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##
# TODO: S3 connection has been established. Now, the data needs to be moved from S3
#  into Snowflake from the existing stages.
#       So:
#           1) Create connection in code to Snowflake that will create the schema and upload the raw data. DONE
#           2) Establish a snowflake session to transform the data and upload it into a clean schema. DONE
#           3) Automate these processes in steps 1 & 2 with MWAA.


# Data transformations
transform = DataTransform


def snowflake_query_exec(queries, method: str = 'standard'):
    try:
        logging = get_run_logger()

        # Cursor & Connection
        conn = get_snowflake_connection(method)
        curs = conn.cursor()

        # Retrieve formatted queries and execute
        response = {}
        for idx, query in queries.items():
            curs.execute_async(query)
            query_id = curs.sfqid
            logging.info(f'Query added to queue: {query_id}')

            curs.get_results_from_sfqid(query_id)

            # IF THE SNOWFLAKE QUERY RETURNS DATA, STORE IT. ELSE, CONTINUE PROCESS.
            result = curs.fetchone()
            df = curs.fetch_pandas_all()

            if result:
                logging.info(f'Query completed successfully and stored: {query_id}')
                response[idx] = result[0]

                if len(df):
                    return df

            else:
                pass

            while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                logging.info(f'Awaiting query completion for {query_id}')
                time.sleep(1)

        return response

    except ProgrammingError as err:
        logging.error(f'Programming Error: {err}')


@task(name="setup")
def setup(
    source: str,
    endpoint: str = "https://www.hockey-reference.com/leagues/",
    year: int = dt.datetime.now().year
):
    logging = get_run_logger()

    # Build endpoint URL & Filenames
    paths = ('seasons', 'playoffs', 'teams')
    if source in paths:
        if source == 'seasons':
            url = f"{endpoint}NHL_{year}_games.html#games"
            filename = f"NHL_{year}_regular_season"
        elif source == 'playoffs':
            url = f"{endpoint}NHL_{year}_games.html#games_playoffs"
            filename = f"NHL_{year}_playoff_season"
        elif source == 'teams':
            url = f"{endpoint}NHL_{year}.html#stats"
            filename = f"NHL_{year}_team_stats"
        else:
            pass

    else:
        logging.error(f'Invalid source specified: {source}')
        sys.exit(1)

    return url, filename


@task(name="file_parser")
def file_parser(source, url, snowflake_conn, year: int = dt.datetime.now().year):
    """ Download raw source data and upload to S3
        Data Source: hockeyreference.com

        TODO: Complete teams statistics and playoff record transformations
    """
    try:
        logging = get_run_logger()
        response = get_legacy_session().get(url)
        dataframes = pd.read_html(response.text)
        dataframe = pd.concat(dataframes, axis=0, ignore_index=True).reset_index(drop=True)

        if source == 'seasons':

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
def s3_parser(filename: str, data: pd.DataFrame, s3_bucket_name: str = 'nhl-data-raw'):
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
        dst, filename = f'{s3_bucket_name}', f'{source}/{filename}.csv'

        # Retrieve S3 paths & store raw file to s3
        logging.info(f'Storing parsed data in S3 at {filename}')
        s3_client.upload_file(
            path, dst, filename
        )
        logging.info(f'Successfully uploaded to S3')

    except Exception as e:
        logging.error(f'An error occurred when storing data in S3: {e}')
        sys.exit(1)


@flow(retries=1, retry_delay_seconds=5, log_prints=True)
def snowflake_ingest(source, endpoint, year, s3_bucket_name, snowflake_conn, env):
    url, filename = setup(source, endpoint, year)
    logging = get_run_logger()

    # Create stages
    snowflake_query_exec(snowflake_stages(), method=snowflake_conn)

    # Create Schemas
    snowflake_query_exec(snowflake_schema(), method=snowflake_conn)

    # Parse Files from Raw Endpoint
    # Only store data in S3 in Production
    if env == "development":
        try:
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
        output_df = file_parser(source, url, snowflake_conn, year)
        s3_parser(filename=filename, data=output_df, s3_bucket_name=s3_bucket_name)

        # DEDUPE FROM SNOWFLAKE
        snowflake_query_exec(snowflake_cleanup(year), method=snowflake_conn)

        # INGEST RAW DATA TO SNOWFLAKE
        snowflake_query_exec(snowflake_ingestion(), method=snowflake_conn)

    end = time.time() - start
    logging.info(f'Process Completed. Time elapsed: {end}')


if __name__ in "__main__":
    start = time.time()
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

    # Execute the pipeline
    snowflake_ingest(source, endpoint, year, s3_bucket_name, snowflake_conn, env)
