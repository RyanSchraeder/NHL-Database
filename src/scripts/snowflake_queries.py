
# LOGGING
from logger import logger

logger = logger('snowflake_queries')


# QUERY EXECUTIONS
def snowflake_stages():
    return {
        "create_parquet": """
            create or replace file format parquet type='parquet'
        """,
        "create_csv": """
            create or replace file format csv type='csv' -- creates the file format to map incoming data structure to CSV
            field_delimiter = ','
            skip_header=1
        """,
        "csv": """
            CREATE OR REPLACE STAGE nhl_raw_data_csv
            STORAGE_INTEGRATION = "aws_s3_integration"
            URL = 's3://nhl-data-raw/'
            -- CREDENTIALS = ''
            FILE_FORMAT = csv
        """,
        "parquet": """
            CREATE OR REPLACE STAGE nhl_raw_data_parquet
            STORAGE_INTEGRATION = "aws_s3_integration"
            URL = 's3://nhl-data-raw/'
            -- CREDENTIALS = ''
            FILE_FORMAT = parquet
        """
}


def snowflake_checks(table):
    return {
        "columns": f"""
                        SELECT column_name FROM NHL_STATS.information_schema.columns
                        WHERE lower(table_name) like '%{table}%'
                    """
    }


def snowflake_schema():
    return {
        "raw_team_stats": """
            create table if not exists raw_team_stats
            (
                Rk integer,
                Team varchar(100),
                AvAge integer,
                GP integer,
                W integer,
                L integer,
                OL integer,
                PTS integer,
                PTS_PERC float,
                GF integer,
                GA integer,
                SOW integer,
                SOL integer,
                SRS integer,
                SOS integer,
                GFVG integer,
                GAVG integer,
                PP integer,
                PPO integer,
                PP_PERC float,
                PPA integer,
                PPOA integer,
                PK_PERC float,
                SH integer,
                SHA integer,
                PIMVG integer,
                oPIMVG integer,
                S integer,
                S_PERC float,
                SA integer,
                SV_PERC float,
                SO integer, 
                updated_at date
            )
        """,
        # "teams": """
        #     create or replace table teams (
        #         team_id autoincrement start 1 increment 1,
        #         team_name varchar(100),
        #         city varchar(100),
        #         state varchar(100)
        #     )
        # """,
        "regular_season": """
            create table if not exists regular_season (
                date date,
                away_team_id varchar(100),
                away_goals integer,
                home_team_id varchar(100),
                home_goals integer,
                length_of_game_min varchar(100),
                updated_at date
            )
        """,
        "playoff_season": """
            create table if not exists playoff_season (
                date date,
                away_team varchar(100),
                away_goals integer,
                home_team varchar(100),
                home_goals integer,
                length_of_game_min integer,
                away_outcome integer,
                home_outcome integer,
                updated_at varchar(100)
            )
        """
    }


def snowflake_cleanup(load_year):
    logger.info(
        f"""
            Cleaning up data with query: \n
            DELETE FROM regular_season
            WHERE date like '{load_year}%'
        """
    )
    return {
        "dedupe_regular_seasons": f"""
            DELETE FROM regular_season
            WHERE date like '{load_year}%'
        """
    }


def snowflake_ingestion():
    logger.info(
        """
        Processing query to ingest data from S3 to Snowflake: 
    
        COPY INTO regular_season
        FROM @nhl_raw_data_csv/season
        FILE_FORMAT = csv
        PATTERN = '.*csv.*';
        """
    )

    return {
        # REGULAR SEASON DATA CLEAN. USES THE S3 INTEGRATION STAGE FOR THE S3 RAW DATA.
        "reg_season_raw": """
                COPY INTO regular_season
                FROM @nhl_raw_data_csv/season
                FILE_FORMAT = csv
                PATTERN = '.*csv.*';
        """
    }