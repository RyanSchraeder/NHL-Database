FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY NHL-Database/ /opt/NHL-Database/
WORKDIR /opt/NHL-Database/

RUN prefect block register -m prefect_aws.ecs

# Run our flow script when the container starts
CMD ["python", "flows/snowflake_transfer.py", "seasons"]
