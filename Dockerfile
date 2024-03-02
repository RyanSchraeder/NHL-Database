FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

WORKDIR /opt/NHL-Database/

# Add our flow code to the image
COPY flows/ /opt/NHL-Database/flows/

RUN ls -ltr
RUN prefect block register -m prefect_aws.ecs

# Run our flow script when the container starts
CMD ["python", "flows/nhl_regular_seasons.py", "seasons"]
