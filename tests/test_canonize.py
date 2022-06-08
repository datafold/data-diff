import unittest
from datetime import datetime, timezone

import preql

from data_diff.database import BigQuery, MySQL, Snowflake, connect_to_uri
from data_diff.sql import Select
from data_diff import database as db

from .common import CONN_STRINGS


DATE_TYPES = {
    db.Postgres: ["timestamp(6) with time zone", "timestamp(6) without time zone"],
    db.MySQL: ["datetime(6)", "timestamp(6)"],
    db.Snowflake: ["timestamp(6)", "timestamp_tz(6)", "timestamp_ntz(6)"],
    db.BigQuery: ["timestamp", "datetime"],
    db.Redshift: ["timestamp(6)", "timestampz(6)"],
    db.Oracle: ["timestamp(n) with timezone", "timestamp(n) with local time zone"],
    db.Presto: ["timestamp", "timestamp with zone"],
}


class TestNormalize(unittest.TestCase):
    i = 0

    def _new_table(self, name):
        self.i += 1
        return f"t_{self.i}"

    def test_normalize(self):
        all_returned = {}

        for db_id, conn_string in CONN_STRINGS.items():
            print(f"Testing {db_id}")

            sample_date1 = datetime(2022, 6, 3, 12, 24, 35, 69296, tzinfo=timezone.utc)
            sample_date2 = datetime(2021, 5, 2, 11, 23, 34, 58185, tzinfo=timezone.utc)
            if db_id in (BigQuery,):
                # TODO Issue when adding timezone to mysql
                dates = [sample_date1, sample_date2.replace(tzinfo=None)]
            else:
                dates = [sample_date1, sample_date2]

            pql = preql.Preql(conn_string)

            date_type_tables = {dt: self._new_table(dt) for dt in DATE_TYPES[db_id]}
            if db_id is BigQuery:
                date_type_tables = {dt: f'data_diff.{name}' for dt, name in date_type_tables.items()}

            used_tables = list(date_type_tables.values())
            conn = None
            try:
                for date_type, table in date_type_tables.items():
                    pql.run_statement(f"DROP TABLE IF EXISTS {table}")
                    pql.run_statement(f"CREATE TABLE {table}(v {date_type})")
                pql.commit()

                for date_type, table in date_type_tables.items():

                    for date in dates:
                        # print(f"insert into {table}(v) values ('{date}')")
                        pql.run_statement(f"insert into {table}(v) values ('{date}')")
                pql.commit()

                conn = connect_to_uri(conn_string)
                assert type(conn) is db_id  # Might change in the future
                if db_id is Snowflake:
                    conn.query("alter session set timestamp_output_format = 'YYYY-MM-DD HH24:MI:SS.FF6TZH'", None)
                    conn.query("alter session set timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF6'", None)

                for date_type, table in date_type_tables.items():
                    schema = conn.query_table_schema(table.split('.'))

                    returned_dates = tuple(
                        x for x, in conn.query(Select([conn.normalize_value_by_type("v", schema["v"])], table), list)
                    )

                    # print("@@", db_id, date_type, " --> ", returned_dates)
                    all_returned[db_id, date_type] = returned_dates

            finally:
                if conn:
                    conn.close()
                for t in used_tables:
                    try:
                        pql.run_statement(f"DROP TABLE {t}")
                    except preql.Signal:
                        pass

        all_reprs = set(all_returned.values())
        # print("@@", all_reprs)
        assert len(all_reprs) == 1
