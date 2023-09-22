-- CREATES THE AWS S3 INTEGRATION

-- CREATE STORAGE INTEGRATION "aws_s3_integration"
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'ARN'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://nhl-data-clean/', 's3://nhl-data-raw/')
--   -- [ STORAGE_BLOCKED_LOCATIONS = ('s3://<bucket>/<path>/', 's3://<bucket>/<path>/') ]

-- DESC INTEGRATION "aws_s3_integration";

-- GRANT CREATE STAGE ON SCHEMA public TO ROLE SYSADMIN;

-- GRANT USAGE ON INTEGRATION "aws_s3_integration" TO ROLE SYSADMIN;

USE SCHEMA NHL_STATS.public;

create or replace file format csv type='csv' -- creates the file format to map incoming data structure to CSV
field_delimiter = ','
skip_header=1
;

create or replace file format parquet type='parquet' -- creates the file format to map incoming data structure to CSV
skip_header=1
;

CREATE STAGE nhl_raw_data
  STORAGE_INTEGRATION = "aws_s3_integration"
  URL = 's3://nhl-data-raw/'
  -- CREDENTIALS = ''
  FILE_FORMAT = csv;

CREATE STAGE nhl_raw_data
  STORAGE_INTEGRATION = "aws_s3_integration"
  URL = 's3://nhl-data-raw/'
  -- CREDENTIALS = ''
  FILE_FORMAT = parquet;

CREATE STAGE nhl_clean_data
  STORAGE_INTEGRATION = "aws_s3_integration"
  URL = 's3://nhl-data-clean/'
  -- CREDENTIALS = ''
  FILE_FORMAT = csv;

list @nhl_raw_data;
list @nhl_clean_data;

-- TEST DATA FILES INTO STAGE
--select $1, $2, $3, $4 from @nhl_raw_data (file_format => csv) test limit 10;

create or replace table team_stats (
    Rk integer,
    Team varchar(100) PRIMARY KEY not null,
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
    SO integer
);
create or replace table raw_team_stats
(
    Rk integer,
    Team varchar(100) PRIMARY KEY not null,
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
    SO integer
);

-- Regular Season Data
create or replace table regular_season (
    date date,
    away_team varchar(100),
    away_goals integer,
    home_team varchar(100),
    home_goals integer,
    length_of_game_min integer,
    away_outcome integer,
    home_outcome integer
);

copy into team_stats
from @nhl_clean_data/teams
file_format=csv
pattern = '.*csv.*';

copy into raw_team_stats
from @nhl_raw_data/teams
file_format=csv
pattern = '.*csv.*';
ON_ERROR = CONTINUE;

copy into regular_season
from @nhl_clean_data/season
file_format=csv
pattern = '.*csv.*';
ON_ERROR = CONTINUE;

-- CREATING FULL STATISTICS TABLE & VIEW

CREATE OR REPLACE VIEW full_statistics_data as (
    SELECT * FROM NHL_STATS.PUBLIC.regular_season rs
    JOIN NHL_STATS.PUBLIC.TEAM_STATS ts
    ON ts.TEAM = rs.away_team
    AND rs.away_outcome = 1

    UNION

    SELECT * FROM NHL_STATS.PUBLIC.regular_season rs
    JOIN NHL_STATS.PUBLIC.TEAM_STATS ts
    ON ts.TEAM = rs.home_team
    AND rs.home_outcome = 1
);

--SELECT * FROM full_statistics_data;


-- Drop if needed
-- DROP TABLE IF EXISTS FULL_STATISTICS_DATASET;
-- DROP TABLE IF EXISTS RAW_TEAM_STATS;