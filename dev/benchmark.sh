#!/bin/bash

run_test() {
    N_SAMPLES=${N_SAMPLES:-1000000} N_THREADS=${N_THREADS:-16} LOG_LEVEL=${LOG_LEVEL:-info} BENCHMARK=1 \
        poetry run python3 -m unittest tests/test_database_types.py -v -k $1
}

run_test "postgresql_int_mysql_int"
run_test "mysql_int_mysql_int"
run_test "postgresql_int_postgresql_int"
run_test "postgresql_ts6_n_tz_mysql_ts0"
run_test "postgresql_ts6_n_tz_snowflake_ts9"
run_test "postgresql_int_presto_int"
run_test "postgresql_int_redshift_int"
run_test "postgresql_int_snowflake_int"
run_test "postgresql_int_bigquery_int"
run_test "snowflake_int_snowflake_int"

poetry run python dev/graph.py
