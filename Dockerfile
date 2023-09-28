FROM python:3.10-alpine
LABEL repo=NHL-Database
WORKDIR NHL-Database/
USER root
RUN apt-get install -y git bash
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install

COPY requirements.txt requirements.txt
COPY src/ src/
RUN pip install -r requirements.txt

RUN which aws
RUN aws --version
