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
from logger import logger

# Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##
# TODO: S3 connection has been established. Now, the data needs to be moved from S3
#  into Snowflake from the existing stages.
#       So:
#           1) Create connection in code to Snowflake that will create the schema and upload the raw data. DONE
#           2) Establish a snowflake session to transform the data and upload it into a clean schema. DONE
#           3) Automate these processes in steps 1 & 2 with MWAA.


# Data transformations
transform = DataTransform


# Logger
logging = logger('snowflake_ingestion')


class SnowflakeIngest(object):

    def __init__(self, source, endpoint, year, s3_bucket_name, snowflake_conn):
        """
            :param s3_bucket_name: The bucket used to access S3 directories
            :param snowflake_conn: The connection method used to connect to Snowflake: str ('standard', 'spark')
        """
        self.endpoint = endpoint
        self.source = source
        self.s3_bucket = s3_bucket_name
        self.conn = get_snowflake_connection(snowflake_conn)
        self.date = dt.date.today()

        if year == "":
            self.year = self.date.year
        else:
            self.year = year

        # Build endpoint URL
        match source:
            case 'seasons':
                self.url = f"{self.endpoint}NHL_{year}_games.html#games"
                self.filename = f"NHL_{year}_regular_season"

            case 'playoffs':
                self.url = f"{self.endpoint}NHL_{year}_games.html#games_playoffs"
                self.filename = f"NHL_{year}_playoff_season"

            case 'teams':
                self.url = f"{self.endpoint}NHL_{year}.html#stats"
                self.filename = f"NHL_{year}_team_stats"

    def file_parser(self):
        """ Download raw source data and upload to S3
            Data Source: hockeyreference.com

            TODO: Complete teams statistics and playoff record transformations
        """
        try:
            response = get_legacy_session().get(self.url)
            dataframes = pd.read_html(response.text)
            dataframe = pd.concat(dataframes, axis=0, ignore_index=True).reset_index(drop=True)

            if self.source == 'seasons':

                logging.info('Transforming data...')
                dataframe = transform.seasons(dataframe, self.date)

                logging.info('Checking column mappings...')
                checks = self.snowflake_query_exec(snowflake_checks('regular_season'))
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

    @s3_conn
    def s3_parser(self, data):
        try:
            # Establish connection
            s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

            # Convert DF to CSV File
            path = f'../data/{self.filename}.csv'
            data.to_csv(path, index=False)

            logging.info(f'Data stored at {path}')

            # Build the targets
            dst, filename = f'{self.s3_bucket}', f'{self.source}/{self.filename}.csv'

            # Retrieve S3 paths & store raw file to s3
            logging.info(f'Storing parsed data in S3 at {filename}')
            s3_client.upload_file(
                path, dst, filename
            )
        except Exception as e:
            logging.error(f'An error occurred when storing data in S3: {e}')
            sys.exit(1)

    def snowflake_query_exec(self, queries):
        try:
            # Cursor & Connection
            curs, conn = self.conn.cursor(), self.conn

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


if __name__ in "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(
        prog="SnowflakeIngestion",
        description="Move data from raw S3 uploads to a produced Schema in Snowflake"
    )

    parser.add_argument('source')
    parser.add_argument('endpoint')
    parser.add_argument('year')
    parser.add_argument('s3_bucket_name')
    parser.add_argument('snowflake_conn')
    parser.add_argument('env')

    args = parser.parse_args()

    source = args.source if args.source is not None else ""
    endpoint = args.endpoint if args.endpoint is not None else ""
    year = args.year if args.year is not None else ""
    # Example S3 URI : s3://nhl-data-raw/season/regular_season.csv
    s3_bucket_name = args.s3_bucket_name if args.s3_bucket_name is not None else ""
    snowflake_conn = args.snowflake_conn if args.snowflake_conn is not None else ""
    env = args.env if args.env is not None else "development"

    # Set up class and ingest data from Raw Source to S3
    execute = SnowflakeIngest(source, endpoint, year, s3_bucket_name, snowflake_conn)

    # CREATE STAGES
    stage = execute.snowflake_query_exec(snowflake_stages())

    # CREATE SCHEMA
    schema = execute.snowflake_query_exec(snowflake_schema())

    # Only store data in S3 in Production
    if env == "development":
        try:
            output_df = execute.file_parser()
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
        output_df = execute.file_parser()
        execute.s3_parser(output_df)

        # DEDUPE FROM SNOWFLAKE
        execute.snowflake_query_exec(snowflake_cleanup(year))

        # INGEST RAW DATA TO SNOWFLAKE
        execute.snowflake_query_exec(snowflake_ingestion())

    end = time.time() - start
    logging.info(f'Process Completed. Time elapsed: {end}')
