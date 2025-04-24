# NHL Data Warehousing - Prefect Stack
Data engineering project utilizing AWS ECS Fargate, CloudFormation (IaC), Snowflake, Docker, Python, SQL, dbt, and Prefect to build a data warehouse in Snowflake for accessibility of NHL data. 

## Architecture
<p align="center">
  <img src="images/NHL Pipeline Diagram.drawio.png" />
</p>

# Development Process
Both `development` and `main` branches are independent, building their own infrastructure within the full stack separated by their naming conventions `development` and `production`. The code is designed to dry-run for `development` while a full ingestion will be executed for `production`. For testing, the dry-run functionality is incorporated into the CI/CD pipeline to ensure the code is running as intended and successfully. In addition, the CI/CD pipeline per each branch is written on GitHub Actions, which are both manually executed and not on a commit basis.

The infrastructure is made on AWS CloudFormation and pushed up into AWS via GitHub Actions, which are all manually triggered pipelines to aid the resource provisioning for the ECS Cluster, ECR Repository, and the ECS Task Definition. When building on `development`, you'll notice a full architecture will be provisioned as well as flows made on Prefect which will run on a cron-based schedule per your deployment. The code executed within the containers which are stored in ECR, however, will be a dry-run. You may assume the opposite when deploying architecture via the `main` branch, where a full ingestion process would execute per the `production` paramater. 

While you _should absolutely separate environments by AWS Account associated with development and production_, this is a personal project, meaning you could technically run `production` code on `development`. However, the separation of the two is simulated in this project to show how environments would actually work. 
In addition to the parameters set for `development` and `production`, a real internal project would have an AWS Account ID in your GitHub Secrets per each branch. 

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

# dbt Cloud
dbt Cloud is used to transform, format, and prepare the complete dataset used by a machine learning model. The data is used from Snowflake to perform curation of a dataset that must be encoded for a classification model, and the SQL within dbt is utilized for that. 
Once the dataset is curated, a post-operation hook is called to train the model within Snowflake using Snowpark on the latest form of the curated dataset. This is scheduled to occur after the data pipelines are all completed. 

# Why Not Managed Airflow? 

#### Short Answer: *Saving 8.5x+ the Cost With Prefect!*
- Additionally, Prefect provides robust integration with existing code to enable smooth application of my custom modules, tests, and configurations without the pain of refactoring to suit Airflow hooks, plugins, etc. 
- Connectivity is seamless and completely customizable with Prefect Blocks
- Workflow is efficient with local testing thanks to Prefect Cloud


### AWS MWAA Airflow Cost Vs. Prefect Architecture:

_Airflow_

<p align="center">
  <img src="/images/with_airflow.png" />
</p>

_Prefect_

<p align="center">
  <img src="/images/no_airflow.png" />
</p>
