#!/usr/bin/env bash
set -ex

# Use a linux/arm64 docker image for MySQL when running on ARM
CPU_ARCHITECTURE=$(uname -p)
if [[ "${CPU_ARCHITECTURE}" == "arm" ]]; then
	MYSQL_IMAGE="arm64v8/mysql:oracle"
fi

main () {
  cd dev/
  initialize 
  prepare_db
  xdiff
  shutdown
  cd ..
}

initialize() {
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d postgres mysql

  until nc -z -v -w30 localhost 3306 && nc -z -v -w30 localhost 5432; do
    echo "Databases not yet ready.."
    sleep 5
  done

  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose up -d xdiff prepdb
}

prepare_db() {
  . ./prepdb.sh
}

xdiff() {
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1 -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1 -e OPTIONS='-c timestamp --bisection-factor 4 -v' xdiff
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update001p -e OPTIONS='-c timestamp --bisection-factor 64 -v -s' xdiff
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
  MYSQL_IMAGE=${MYSQL_IMAGE} docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update50p -e OPTIONS='-c timestamp --bisection-factor 4 -v -s' xdiff
}

shutdown() {
  docker-compose down
}

main
