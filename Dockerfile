FROM 3.9-slim
LABEL repo=NHL-Database
USER root

RUN  apt-get -yq update && \
     apt-get -yqq install git && \
     apt-get -yqq install ssh

COPY requirements.txt requirements.txt
COPY src/ src/
RUN pip install -r requirements.txt
