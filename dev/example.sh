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
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d postgres mysql

  until nc -z -v -w30 localhost 3306 && nc -z -v -w30 localhost 5432; do
    echo "Databases not yet ready.."
    sleep 5
  done

  docker-compose up -d xdiff prepdb
}

prepare_db() {
  . ./prepdb.sh
}

xdiff() {
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1 -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1 -e OPTIONS='-c timestamp --bisection-factor 4 -v' xdiff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update001p -e OPTIONS='-c timestamp --bisection-factor 64 -v -s' xdiff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update50p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
}

shutdown() {
  docker-compose down
}

main
