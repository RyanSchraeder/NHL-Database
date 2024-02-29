# NHL-Database
Data engineering project utilizing AWS, Snowflake, Docker, Python and Prefect to build a data warehouse in Snowflake for accessibility of NHL data. 

## Workflow Architecture
1. In Python, source data is retrieved from hockeyreference.com based upon an input parameter to retrieve the appropriate URL, which is then used to scrape data and transform it into tabular format (dataframe).
2. The capability of the application involves source checks on columns, minor transformations into correct data types, logging, and schema definition. The schemas and Snowflake Stages are updated to appropriate structure to prepare acceptance of incoming data later on.
3. Within the Python script, connectors are validated for secure and proper connection to external sources Snowflake and AWS S3 followed by transport to AWS S3, destructively overwriting files in place to ensure idempotency of storage. 
  > Ex. The dataframe output is saved with the file convention "NHL_{YYYY}_regular_season.csv". If a file of the current respective type exists, for instance the NHL regular season of the current year, then the latest run will overwrite the file in S3 to avoid duplication.
4. After the prepared data is successfully stored in AWS S3, the script will check for the updated file and prepare Snowflake for acceptance of the data. This step involves clearing out existent data of the current year and ingestion type before loading updated data. In other words, if you request the pipeline to load the `source` of "seasons", the pipeline will retrieve regular season data for the year you specify (or take the current year if not passed by default) and overwrite such data in the Snowflake location respectively.

#### Possible Outcomes

Parameters for the pipeline are established:

| Argument | Description |
| -------- | ----------- |
| source   | Form of data you wish to load. Varies by "seasons" for regular season, "playoffs" for the playoff season, and "teams" for team statistics. |
| endpoint | The endpoint to request data from. This defaults to the regular season, but will be changed based upon the `source` provided. | 
| year | Year of which to process data. Defaults to the current year at runtime |
| s3_bucket_name | The name of the S3 Bucket location for storage of the output data. Defaults to the `nhl-data-raw` storage location and directory based upon `source` | 
| snowflake_conn | Connection method for Snowflake. Optionally 'standard' or 'snowpark'. Snowpark capability will start a spark sesssion for connection to Snowpark. Defaults to a standard Snowflake Connector, falls back to a Prefect Snowflake Block.
> **NOTE: Currently only works with a Prefect Snowflake Block via ECS due to internal Python Snowflake connector issues and is limited to methods in the Prefect Snowflake library.** |
| env | Environment connection. Default to 'development'. No data will be loaded in the development environment. |


<p align="center">
  <img src="/images/flow_diagram.png" />
</p>


## Pipeline Tasks in Prefect

<p align="center">
  <img src="/images/task_runs.png" />
</p>

### Task Logs Example

<p align="center">
  <img src="/images/prefect_logging.png" />
</p>

### Prefect Dashboard

<p align="center">
  <img src="/images/prefect_dashboard.png" />
</p>

# Output Data in Snowflake

<p align="center">
  <img src="/images/snowflake_regular_season.png" />
</p>


#### WIP
1. Finish the ingestion sources
2. Establish data models and transformation in the next phase within dbt Cloud
3. Utilize data to build a Streamlit dashboard via Snowflake
4. Utilize data to productionalize the initial Stanley Cup Predictions model
5. Explore streaming architecture possibilities for realtime predictions
