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
  prepaire_db
  xdiff
  shutdown
}

initialize() {
  poetry install
  pip install preql==0.2.10 # Temporary due to version conflicts for runtype

  if [ ! -f ./ml-25m/ratings.csv ]; then
    echo "Example data not found. Downloading.."
    wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
    unzip ml-25m.zip
  fi
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d
  sleep 5
}

prepaire_db() {
    preql -m prepare_db mysql://mysql:Password1@localhost/mysql
    preql -m prepare_db postgres://postgres:Password1@localhost/postgres
}

xdiff() {
    xdiff postgres://postgres:Password1@localhost/postgres Rating mysql://mysql:Password1@localhost/mysql Rating_del1p -c timestamp --stats
}

shutdown() {
    docker-compose down
}

main
