FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

# Add our flow code to the image
COPY src/ .

# Run our flow script when the container starts
CMD ["python", "scripts/snowflake_transfer.py", "seasons"]
