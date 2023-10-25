FROM mageai/mageai:latest

ARG PROJECT_NAME="NHL-Database"
ARG CODE_PATH=/home/NHL-Database
ARG USER_CODE_PATH=${CODE_PATH}/${PROJECT_NAME}

WORKDIR ${CODE_PATH}

LABEL repo=NHL-Database
USER root

RUN  apt-get -yq update && \
     apt-get -yqq install git && \
     apt-get -yqq install ssh

ENV USER_CODE_PATH=${USER_CODE_PATH}

COPY ${PROJECT_NAME} ${PROJECT_NAME}

RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt
ENV PYTHONPATH="${PYTHONPATH}:/home/NHL-Database"

CMD ["/bin/sh", "-c", "/app/run_app.sh"]
