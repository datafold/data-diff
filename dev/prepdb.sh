#!/usr/bin/env bash
set -ex

main () {
  prepare_db
}

prepare_db() {
  START=$(date +%s)
  poetry run preql -m prepare_db mysql://mysql:Password1@mysql/mysql
  END=$(date +%s)
  DIFF=$(echo "$END - $START" | bc)
  echo "Prepare_db for mysql took: $DIFF s"
  START=$(date +%s)
  poetry run preql -m prepare_db postgres://postgres:Password1@postgresql/postgres
  END=$(date +%s)
  DIFF=$(echo "$END - $START" | bc)
  echo "Prepare_db for postgres took: $DIFF s"
}

main