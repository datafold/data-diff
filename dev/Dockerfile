FROM python:3.10

# install dependencies
RUN apt update && apt install -y \
    python3-dev libpq-dev wget unzip \
    python3-setuptools gcc bc
RUN pip install poetry

ADD . /app

WORKDIR /app
RUN chmod +x dev/prepdb.sh

RUN wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
RUN unzip ml-25m.zip -d /app/dev

RUN pip install 'data-diff[preql,mysql,pgsql]'

ARG DB1_URI
ARG TABLE1_NAME
ARG DB2_URI
ARG TABLE2_NAME
ARG OPTIONS

ENV DB1_URI ${DB1_URI}
ENV TABLE1_NAME ${TABLE1_NAME}
ENV DB2_URI ${DB2_URI}
ENV TABLE2_NAME ${TABLE2_NAME}
ENV OPTIONS ${OPTIONS}

CMD data_diff ${DB1_URI} ${TABLE1_NAME} ${DB2_URI} ${TABLE2_NAME} ${OPTIONS}