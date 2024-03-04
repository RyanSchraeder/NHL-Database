# QUERY EXECUTIONS
def snowflake_stages(db: str, schema: str, s3_bucket_name: str):
    queries = {
        "use_db": f"""
            use database {db}
        """,
        "create_schema": f"""
            create schema if not exists {schema};
        """,
        "use_schema": f"""
            use schema {schema};
        """,
        "create_parquet": f"""
            create file format if not exists parquet type='parquet';
        """,
        "create_csv": f"""
            create file format if not exists csv type='csv' -- creates the file format to map incoming data structure to CSV
            field_delimiter = ','
            skip_header=1
        """,
        "csv": f"""
            CREATE STAGE IF NOT EXISTS {db}.{schema}.nhl_raw_data_csv
            STORAGE_INTEGRATION = "aws_s3_integration"
            URL = 's3://{s3_bucket_name}/'
            -- CREDENTIALS = ''
            FILE_FORMAT = csv
        """,
        "parquet": f"""
            CREATE STAGE IF NOT EXISTS {db}.{schema}.nhl_raw_data_parquet
            STORAGE_INTEGRATION = "aws_s3_integration"
            URL = 's3://{s3_bucket_name}/'
            -- CREDENTIALS = ''
            FILE_FORMAT = parquet
        """
    }
    return queries


def snowflake_checks(db: str, table: str):
    return {
        "columns": f"""
            SELECT DISTINCT column_name FROM {db}.information_schema.columns
            WHERE lower(table_name) like '%{table}%'
        """
    }


def snowflake_schema(db, schema):
    queries = {
        "use_db": f"use database {db};",
        "create_schema": f"create schema if not exists {schema};",
        "use_schema": f"use schema {schema};",
        "team_stats": f"""
            create table if not exists {db}.{schema}.team_stats (
                Team VARCHAR,
                GP VARCHAR,
                W VARCHAR,
                L VARCHAR,
                OL VARCHAR,
                PTS VARCHAR,
                "PTS%" VARCHAR,
                GF VARCHAR,
                GA VARCHAR,
                SRS VARCHAR,
                SOS VARCHAR,
                "RPt%" VARCHAR,
                RW VARCHAR,
                RgRec VARCHAR,
                "RgPt%" VARCHAR,
                updated_at date
            );
        """,
        "regular_season": f"""
            create table if not exists {db}.{schema}.regular_season (
                date date,
                away_team_id varchar(100),
                away_goals integer,
                home_team_id varchar(100),
                home_goals integer,
                length_of_game_min varchar(100),
                updated_at date
            )
        """,
        "playoff_season": f"""
            create table if not exists {db}.{schema}.playoff_season (
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
    return queries


def snowflake_cleanup(db, schema, table, load_year):

    if table == 'team_stats':
        print('Team stats do not need deletion from a season date as they are refreshed records. Not deleting records by season date. Passing')
        return {}
    else:
        print(
            f"""
            Cleaning up data with query: \n
            DELETE FROM {db}.{schema}.{table}
            WHERE date like '{load_year}%'
            """
        )
        queries = {
            "dedupe": f"""
                DELETE FROM {db}.{schema}.{table}
                WHERE updated_date like '{load_year}%'
            """
        }
        return queries 


def snowflake_ingestion(db, schema, table, source):

    queries = {
        "ingest_from_stage": f"""
            COPY INTO {db}.{schema}.{table}
            FROM @nhl_raw_data_csv/{source}/
            FILE_FORMAT = csv
            PATTERN = '.*csv.*';
        """
    }
    print(f"Query prepared for ingestion from external stage: \n\t{queries['ingest_from_stage']}")

    return queries
