# QUERY EXECUTIONS
def snowflake_stages():
    return {
        "create_parquet": """
            create file format if not exists parquet type='parquet'
        """,
        "create_csv": """
            create file format if not exists csv type='csv' -- creates the file format to map incoming data structure to CSV
            field_delimiter = ','
            skip_header=1
        """,
        "csv": """
            CREATE STAGE IF NOT EXISTS nhl_raw_data_csv
            STORAGE_INTEGRATION = "aws_s3_integration"
            URL = 's3://nhl-data-raw/'
            -- CREDENTIALS = ''
            FILE_FORMAT = csv
        """,
        "parquet": """
            CREATE STAGE IF NOT EXISTS nhl_raw_data_parquet
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
        #     create table if not exists teams (
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
                away_team_id varchar(100),
                away_goals integer,
                home_team_id varchar(100),
                home_goals integer,
                length_of_game_min varchar(100),
                updated_at date
            )
        """
    }


def snowflake_cleanup(table, load_year):
    print(
        f"""
        Cleaning up data with query: \n
        DELETE FROM {table}
        WHERE date like '{load_year}%'
         """
    )
    queries = {
        "dedupe": f"""
               DELETE FROM  {table}
               WHERE date like '{load_year}%'
           """
    }

    return queries


def snowflake_ingestion(table, source):
    print(f"Processing query to ingest data from S3 to Snowflake: {table}")

    # REGULAR SEASON DATA CLEAN. USES THE S3 INTEGRATION STAGE FOR THE S3 RAW DATA.
    queries = {
        "ingest_from_stage": f"""
            COPY INTO {table}
            FROM @nhl_raw_data_csv/{source}/
            FILE_FORMAT = csv
            PATTERN = '.*csv.*';
        """
    }
    print(f"Query for ingestion from stage: \n\t{queries['ingest_from_stage']}")

    return queries
