FROM python:3.10
RUN apt-get update && apt-get install -y \
    python3-dev libpq-dev wget unzip \
    python3-setuptools gcc bc
RUN pip install --no-cache-dir poetry==1.1.13
COPY . /app
WORKDIR /app
# For now while we are in heavy development we install the latest with Poetry
# and execute directly with Poetry. Later, we'll move to the released Pip package.
RUN poetry install
ENTRYPOINT ["poetry", "run", "python3", "-m", "data_diff"]
