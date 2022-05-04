FROM python:3.10

# install dependencies
RUN apt update && apt install -y \
    python3-dev libpq-dev wget unzip \
    python3-setuptools gcc bc
RUN pip install poetry

# retrieve data
RUN wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
RUN unzip ml-25m.zip -d /app

# copy data
COPY /dev/prepare_db.pql /app/prepare_db.pql
COPY /dev/prepare_db_mssql.pql /app/prepare_db_mssql.pql
COPY /dev/prepare_db_bigquery.pql /app/prepare_db_bigquery.pql
COPY /dev/poetry.lock /dev/pyproject.toml /dev/prepdb.sh /app/
ADD src src

WORKDIR /app
RUN chmod +x prepdb.sh

# install package
RUN poetry install

CMD ["bash", "./prepdb.sh"]