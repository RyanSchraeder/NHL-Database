<<<<<<< Updated upstream
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY src/ .

# Run our flow script when the container starts
CMD ["python", "scripts/snowflake_transfer.py", "seasons"]
=======
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY NHL-Database/ /opt/NHL-Database/
WORKDIR src/

RUN prefect block register -m nhl_pipeline_prefect_block.ecs

# Run our flow script when the container starts
CMD ["python", "scripts/snowflake_transfer.py", "seasons"]
>>>>>>> Stashed changes
