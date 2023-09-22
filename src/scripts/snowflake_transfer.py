import pandas as pd
import argparse
import requests
from io import StringIO
import json
import datetime as dt
import time
import sys
import os
import boto3
from secrets_access import get_secret
from typing import List, Any, Optional
from logger import logger
from connectors import get_snowflake_connection, s3_conn
from snowflake.connector import ProgrammingError
from connectors import get_legacy_session
from snowflake_queries import snowflake_stages, snowflake_schema, snowflake_ingestion

# Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##
# TODO: S3 connection has been established. Now, the data needs to be moved from S3
#  into Snowflake from the existing stages.
#       So:
#           1) Create connection in code to Snowflake that will create the schema and upload the raw data.
#             2) Establish a Snowpark session to transform the data and upload it into a clean schema.
#               3) Automate these processes in steps 1 & 2 with MWAA.


# LOGGING
logger = logger('snowflake_transfer')


class SnowflakeIngest(object):

    def __init__(self, source, endpoint, s3_bucket_name, snowflake_conn):
        """
            :param s3_bucket_name: The bucket used to access S3 directories
            :param snowflake_conn: The connection method used to connect to Snowflake: str ('standard', 'spark')
        """
        self.endpoint = endpoint
        self.source = source
        self.s3_bucket = s3_bucket_name
        self.conn = get_snowflake_connection(snowflake_conn)

        self.curr_date = dt.date.today()
        year = self.curr_date.year

        # Build endpoint URL
        if source == 'seasons':
            self.url = f"{self.endpoint}NHL_{year}_games.html#games"
            self.filename = f"NHL_{year}_regular_season"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/season/'
        elif source == 'playoffs':
            self.url = f"{self.endpoint}NHL_{year}_games.html#games_playoffs"
            self.filename = f"NHL_{year}_playoff_season"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/playoffs/'
        elif source == 'teams':
            self.url = f"{self.endpoint}NHL_{year}.html#stats"
            self.filename = f"NHL_{year}_team_stats"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/playoffs/'

        # Run raw ingestion of data from source
        output_df = self.file_parser()
        self.s3_parser(output_df)

    def file_parser(self):
        """ Download raw source data and upload to S3
            Data Source: hockeyreference.com

            TODO: Complete teams statistics and playoff record transformations
        """
        try:
            response = get_legacy_session().get(self.url)
            dataframes = pd.read_html(response.text)

            dataframe = pd.concat(dataframes, axis=0, ignore_index=False)

            if self.source == 'seasons':
                dataframe = dataframe.rename(columns=({
                    'Date': 'date', 'Visitor': 'away_team_id', 'Home': 'home_team_id', 'G': 'away_goals', 'G.1': 'home_goals',
                    'LOG': 'length_of_game_min'
                }))
                dataframe = dataframe[
                    ['date', 'away_team_id', 'away_goals', 'home_team_id', 'home_goals', 'length_of_game_min']
                ]

                dataframe['updated_at'] = self.curr_date

                # Transforming data
                dataframe['length_of_game_min'] = [i.replace(':', '') for i in dataframe['length_of_game_min']]
                dataframe['length_of_game_min'] = [(int(i[0]) * 60) + int(i[1:]) for i in dataframe['length_of_game_min']]

                dataframe.date = dataframe.date.apply(pd.to_datetime)

            if self.source == 'teams':
                # Team name cleaning
                dataframe['Team'] = [str(i).replace('*', '') for i in dataframe['Team']]

                # Creating Column for Total Goals

                dataframe['G'] = dataframe.GF + dataframe.GA

                # Creating Column for Total Power-Play Goals

                dataframe['PPG'] = dataframe.PP + dataframe.PPA

                # Creating Column for Total Games in Shootouts

                dataframe['SHOOTOUTS'] = dataframe.SOW + dataframe.SOL

                def percents(df):
                    for column, row in df.iteritems():
                        if '%' in column:
                            for item in row:
                                if item < 1:
                                    row += row * 100

                    return df

                team_stats_df = percents(dataframe)

            # if self.source == 'playoffs'

            logger.info(
                f'Retrieved data with columns: {dataframe.columns}'
                f'\n'
                f'Preview: \n{dataframe.head(3)}'
            )

            return dataframe

        except Exception as e:
            logger.error(f'An error occurred while retrieving raw data: {e}')

    @s3_conn
    def s3_parser(self, data):
        try:
            # Establish connection
            s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

            # Convert DF to CSV File
            path = f'../data/{self.filename}.csv'
            data.to_csv(path, index=False)

            # Build the targets
            dst, filename = f'{self.s3_bucket}', f'{self.source}/{self.filename}.csv'

            # Retrieve S3 paths & store raw file to s3
            logger.info(f'Storing parsed data in S3 at {filename}')
            s3_client.upload_file(
                path, dst, filename
            )
        except Exception as e:
            logger.error(f'An error occurred when storing data in S3: {e}')
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
                logger.info(f'Query added to queue: {query_id}')

                curs.get_results_from_sfqid(query_id)

                # IF THE SNOWFLAKE QUERY RETURNS DATA, STORE IT. ELSE, CONTINUE PROCESS.
                result = curs.fetchone()
                if result:
                    logger.info(f'Query completed successfully and stored: {query_id}')
                    response[idx] = result[0]
                else:
                    pass

                while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                    logger.info(f'Awaiting query completion for {query_id}')
                    time.sleep(1)

            return response

        except ProgrammingError as err:
            logger.error(f'Programming Error: {err}')


if __name__ in "__main__":

    parser = argparse.ArgumentParser(
        prog="SnowflakeIngestion",
        description="Move data from raw S3 uploads to a produced Schema in Snowflake"
    )

    parser.add_argument('source')
    parser.add_argument('endpoint')
    parser.add_argument('s3_bucket_name')
    parser.add_argument('snowflake_conn')

    args = parser.parse_args()

    source = args.source if args.source is not None else ""
    endpoint = args.endpoint if args.endpoint is not None else ""

    # Example S3 URI : s3://nhl-data-raw/season/regular_season.csv
    s3_bucket_name = args.s3_bucket_name if args.s3_bucket_name is not None else ""
    snowflake_conn = args.snowflake_conn if args.snowflake_conn is not None else ""

    # Set up class and ingest data from Raw Source to S3
    execute = SnowflakeIngest(source, endpoint, s3_bucket_name, snowflake_conn)

    # CREATE STAGES
    execute.snowflake_query_exec(snowflake_stages())

    # CREATE SCHEMA
    schema = execute.snowflake_query_exec(snowflake_schema())

    # INGEST RAW DATA
    execute.snowflake_query_exec(snowflake_ingestion())
