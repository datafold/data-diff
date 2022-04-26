#!/usr/bin/env bash
set -ex

# Use a linux/arm64 docker image for MySQL when running on ARM
CPU_ARCHITECTURE=$(uname -p)
if [[ "${CPU_ARCHITECTURE}" == "arm" ]]; then
	MYSQL_IMAGE="arm64v8/mysql:oracle"
fi

main () {
  initialize 
  prepaire_db
  xdiff
  shutdown
}

initialize() {
  poetry install

  if [ ! -f ./ml-25m/ratings.csv ]; then
    echo "Example data not found. Downloading.."
    wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
    unzip ml-25m.zip
  fi
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d
  sleep 5
}

prepaire_db() {
    poetry run preql -m prepare_db mysql://mysql:Password1@localhost/mysql
    poetry run preql -m prepare_db postgres://postgres:Password1@localhost/postgres
}

xdiff() {
    poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_del1p -c timestamp --stats
}

shutdown() {
    docker-compose down
}

main
