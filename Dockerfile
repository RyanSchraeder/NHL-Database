FROM python:3.9

WORKDIR /home/NHL-Database

LABEL repo=NHL-Database
USER root

RUN  apt-get -yq update && \
     apt-get -yqq install git && \
     apt-get -yqq install ssh

COPY requirements.txt requirements.txt
COPY src/ src/
RUN pip install -r --no-cache-dir requirements.txt

CMD [
        "python", "src/scripts/snowflake_transfer.py",
        "--source", "seasons",
        "--endpoint", "https://www.hockey-reference.com/leagues/",
        "--year", "2023",
        "--s3_bucket_name", "nhl-data-raw",
        "--snowflake_conn", "standard"
    ]
