import time
import sys
import os
import datetime as dt

# For casting query results to a Pandas DataFrame
import pandas as pd

# Prefect logger
from prefect import get_run_logger

# Snowflake Connections
from snowflake.connector import ProgrammingError
from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector
from src.connectors import get_snowflake_connection


def setup(
        source: str,
        endpoint: str = "https://www.hockey-reference.com/leagues/",
        year: int = dt.datetime.now().year
):
    """
    Setup function that builds the URL for request at hockeyreference.com. Accepts the endpoint parameter and source to generate the appropriate URL.
        :param: source -> type of data to extract. 'seasons', 'playoffs', 'teams' stored in the paths variable within the function.
        :param: endpoint -> base url that is passed to the pipeline via the endpoint parameter. Default: https://www.hockey-reference.com/leagues/
        :year: year -> year of which to process data from. Default: current date at runtime
    """

    logging = get_run_logger()
    logging.info(f'Received endpoint {endpoint} for source {source} and year {year}.')

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

    logging.info(f"URL Built: {url}\nDestination Filename: {filename}")
    return url, filename


def snowflake_query_exec(queries, method: str = 'standard'):
    logging = get_run_logger()
    try:
        # Cursor & Connection
        conn = get_snowflake_connection(method)
        logging.info(f"Snowflake connection established: {conn}")

        response = {}

        if conn:
            curs = conn.cursor()

            # Retrieve formatted queries and execute - Snowflake Connector Form. Async
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

                while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                    logging.info(f'Awaiting query completion for {query_id}')
                    time.sleep(1)

                return response

        if not conn:
            # Retrieve formatted queries and execute - Fallback: Prefect Snowflake Connector. Sync
            # Prefect Snowflake Connector
            logging.warning(f"Snowflake cursor is empty! Attempting Prefect Connector.")
            credentials = SnowflakeCredentials.load("development")

            with SnowflakeConnector.load("development") as cnx:
                for idx, query in queries.items():
                    while True:

                        logging.info(
                            f"""
                            Executing Query: \n'
                            \t\t{query}\n
                            """
                        )
                        result = cnx.fetch_one(query)
                        # full_result = cnx.fetch_all()

                        logging.info(f'Query Result from Prefect Snowflake: {result}')

                        if result:
                            logging.info(f'Query completed successfully and stored: {query}')
                            response[idx] = result[0]
                            if len(result):
                                return result

    except ProgrammingError as err:
        logging.error(f'Programming Error: {err}')
