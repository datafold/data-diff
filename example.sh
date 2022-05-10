#!/usr/bin/env bash
set -ex

main () {
  cd dev/
  initialize 
  prepare_db
  data_diff
  shutdown
  cd ..
}

initialize() {
  docker-compose up -d postgres mysql

  until nc -z -v -w30 localhost 3306 && nc -z -v -w30 localhost 5432; do
    echo "Databases not yet ready.."
    sleep 5
  done

  docker-compose up -d data-diff prepdb
}

prepare_db() {
  . ./prepdb.sh
}

data_diff() {
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1 -e OPTIONS='-t timestamp --bisection-factor 4 -v -s' data-diff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1 -e OPTIONS='-t timestamp --bisection-factor 4 -v' data-diff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update001p -e OPTIONS='-t timestamp --bisection-factor 64 -v -s' data-diff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update1p -e OPTIONS='-t timestamp --bisection-factor 4 -v -s' data-diff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_del1p -e OPTIONS='-t timestamp --bisection-factor 4 -v -s' data-diff
  docker-compose run -e DB1_URI=postgres://postgres:Password1@postgresql/postgres -e TABLE1_NAME=Rating -e DB2_URI=mysql://mysql:Password1@mysql/mysql -e TABLE2_NAME=Rating_update50p -e OPTIONS='-t timestamp --bisection-factor 4 -v -s' data-diff
}

shutdown() {
  docker-compose down
}

main
