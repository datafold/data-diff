#!/usr/bin/env bash
set -ex

main () {
  cd dev
#   get_data
  prepare_db
  cd ..
}

get_data() {
  wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
  unzip ml-25m.zip
}

prepare_db() {
  START=$(date +%s)
  preql -m prepare_db mysql://mysql:Password1@mysql/mysql
  END=$(date +%s)
  DIFF=$(echo "$END - $START" | bc)
  echo "Prepare_db for mysql took: $DIFF s"
  START=$(date +%s)
  preql -m prepare_db postgres://postgres:Password1@postgresql/postgres
  END=$(date +%s)
  DIFF=$(echo "$END - $START" | bc)
  echo "Prepare_db for postgres took: $DIFF s"
}

main