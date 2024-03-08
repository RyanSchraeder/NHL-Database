# NHL-Database
Data engineering project utilizing AWS ECS Fargate, CloudFormation (IaC), Snowflake, Docker, Python, SQL, and Prefect to build a data warehouse in Snowflake for accessibility of NHL data. 

## Architecture Flow Diagram (Click to Expand)
<p align="center">
  <img src="/images/nhl_flow_diagram.png" />
</p>

## Workflow Process
1. In Python, source data is retrieved from hockeyreference.com based upon an input parameter to retrieve the appropriate URL, which is then used to scrape data and transform it into tabular format (dataframe).
2. The capability of the application involves source checks on columns, minor transformations into correct data types, logging, and schema definition. The schemas and Snowflake Stages are updated to appropriate structure to prepare acceptance of incoming data later on.
3. Within the Python script, connectors are validated for secure and proper connection to external sources Snowflake and AWS S3 followed by transport to AWS S3, destructively overwriting files in place to ensure idempotency of storage. 
  > Ex. The dataframe output is saved with the file convention "NHL_{YYYY}_regular_season.csv". If a file of the current respective type exists, for instance the NHL regular season of the current year, then the latest run will overwrite the file in S3 to avoid duplication.
4. After the prepared data is successfully stored in AWS S3, the script will check for the updated file and prepare Snowflake for acceptance of the data. This step involves clearing out existent data of the current year and ingestion type before loading updated data. In other words, if you request the pipeline to load the `source` of "seasons", the pipeline will retrieve regular season data for the year you specify (or take the current year if not passed by default) and overwrite such data in the Snowflake location respectively.

### Parameterization

Parameters for the pipeline are established:

| Argument | Description |
| -------- | ----------- |
| source   | Form of data you wish to load. Varies by "seasons" for regular season and "teams" for team statistics. |
| endpoint | The endpoint to request data from. This defaults to the regular season, but will be changed based upon the `source` provided. | 
| year | Year of which to process data. Defaults to the current year at runtime |
| s3_bucket_name | The name of the S3 Bucket location for storage of the output data. Defaults to the `nhl-data-raw` storage location and directory based upon `source` | 
| snowflake_conn | Connection method for Snowflake. Optionally 'standard' or 'snowpark'. Snowpark capability will start a spark sesssion for connection to Snowpark. Defaults to a standard Snowflake Connector, falls back to a Prefect Snowflake Block. **NOTE: Currently only works with a Prefect Snowflake Block via ECS due to internal Python Snowflake connector issues and is limited to methods in the Prefect Snowflake library.** |
| env | Environment connection. Default to 'development'. No data will be loaded in the development environment. |


## Orchestration
### _Pipeline Tasks in Prefect_
> #### _Development_
 <p align="center">
  <img src="/images/prefectdev.png" />
 </p>

> #### _Production_
<p align="center">
  <img src="/images/task_runs.png" />
</p>

### Task Logs Example
> #### _Development_
<p align="center">
  <img src="/images/prefectdevlogs.png" />
</p>
 
> #### _Production_
<p align="center">
  <img src="/images/prefect_logging.png" />
</p>

### Prefect Dashboard

![image](https://github.com/RyanSchraeder/NHL-Database/assets/30241666/00f739ae-cb1f-4c70-9b35-2b3c71e7d288)


## Output Data in Snowflake 

### Seasonal Games
<p align="center">
  <img src="/images/snowflake_regular_season.png" />
</p>

#### Team Statistics
<p align="center">
  <img src="/images/" />
</p>

# Why Not Airflow? 

#### Short Answer: *Saving 8.5x+ the Cost With Prefect!*
- Additionally, Prefect provides robust integration with existing code to enable smooth application of my custom modules, tests, and configurations without the pain of refactoring to suit Airflow hooks, plugins, etc. 
- Connectivity is seamless and completely customizable with Prefect Blocks
- Workflow is efficient with local testing thanks to Prefect Cloud


### Airflow Cost Vs. Prefect Architecture:

_Airflow_

<p align="center">
  <img src="/images/with_airflow.png" />
</p>

_Prefect_

<p align="center">
  <img src="/images/no_airflow.png" />
</p>



#### WIP
1. Utilize data to build a Streamlit dashboard via Snowflake
2. Utilize data to productionalize the initial Stanley Cup Predictions model
3. Explore streaming architecture possibilities for realtime predictions
