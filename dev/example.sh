#!/usr/bin/env bash
set -ex

if [[ $( dirname $0 ) != "." ]]; then
  echo "Execute from /dev folder."
  exit
fi

# Use a linux/arm64 docker image for MySQL when running on ARM
CPU_ARCHITECTURE=$(uname -p)
if [[ "${CPU_ARCHITECTURE}" == "arm" ]]; then
	MYSQL_IMAGE="arm64v8/mysql:oracle"
fi

main () {
  initialize 
  prepare_db
  xdiff
  shutdown
}

initialize() {
  pip install poetry
  poetry install

  if [ ! -f ./ml-25m/ratings.csv ]; then
    echo "Example data not found. Downloading.."
    wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
    unzip ml-25m.zip
  fi
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d

  until nc -z -v -w30 localhost 3306 && nc -z -v -w30 localhost 5432; do
    echo "Databases not yet ready.."
    sleep 5
  done
}

prepare_db() {
  poetry run preql -m prepare_db mysql://mysql:Password1@localhost/mysql
  poetry run preql -m prepare_db postgres://postgres:Password1@localhost/postgres
}

xdiff() {
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_del1 -c timestamp --stats -v
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_update1 -c timestamp --stats -v
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_update001p -c timestamp --stats -v
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_update1p -c timestamp --stats -v
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_del1p -c timestamp --stats -v
  poetry run xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_update50p -c timestamp --stats -v
}

shutdown() {
  docker-compose down
}

main
