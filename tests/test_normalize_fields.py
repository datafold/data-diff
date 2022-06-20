from concurrent.futures import ThreadPoolExecutor
import unittest
from datetime import datetime, timezone
import logging

import preql

from data_diff.database import BigQuery, MySQL, Snowflake, connect_to_uri, Oracle
from data_diff.sql import Select
from data_diff import database as db

from .common import CONN_STRINGS

logger = logging.getLogger()

DATE_TYPES = {
    db.Postgres: ["timestamp({p}) with time zone", "timestamp({p}) without time zone"],
    db.MySQL: ["datetime({p})", "timestamp({p})"],
    db.Snowflake: ["timestamp({p})", "timestamp_tz({p})", "timestamp_ntz({p})"],
    db.BigQuery: ["timestamp", "datetime"],
    db.Redshift: ["timestamp", "timestamp with time zone"],
    db.Oracle: ["timestamp({p}) with time zone", "timestamp({p}) with local time zone"],
    db.Presto: ["timestamp", "timestamp with zone"],
}


class TestNormalize(unittest.TestCase):
    i = 0

    def _new_table(self, name):
        self.i += 1
        return f"t_{self.i}"

    def _test_dates_for_db(self, item, precision=3):
        db_id, conn_string = item

        logger.info(f"Testing {db_id}")

        sample_date1 = datetime(2022, 6, 3, 12, 24, 35, 69296, tzinfo=timezone.utc)
        sample_date2 = datetime(2021, 5, 2, 11, 23, 34, 500001, tzinfo=timezone.utc)
        sample_date3 = datetime(2021, 5, 2, 11, 23, 34, 000000, tzinfo=timezone.utc)

        dates = [sample_date1, sample_date2, sample_date3]
        if db_id in (BigQuery, Oracle):
            # TODO BigQuery doesn't seem to support timezone for datetime
            dates = [d.replace(tzinfo=None) for d in dates]

        pql = preql.Preql(conn_string)

        date_types = [t.format(p=precision) for t in DATE_TYPES[db_id]]
        date_type_tables = {dt: self._new_table(dt) for dt in date_types}
        if db_id is BigQuery:
            date_type_tables = {dt: f"data_diff.{name}" for dt, name in date_type_tables.items()}
        elif db_id is MySQL:
            pql.run_statement("SET @@session.time_zone='+00:00'")

        used_tables = list(date_type_tables.values())
        conn = None
        results = []
        try:

            for date_type, table in date_type_tables.items():
                if db_id is not Oracle:
                    pql.run_statement(f"DROP TABLE IF EXISTS {table}")
                pql.run_statement(f"CREATE TABLE {table}(id int, v {date_type})")
            pql.commit()

            for date_type, table in date_type_tables.items():

                for index, date in enumerate(dates, 1):
                    # print(f"insert into {table}(v) values ('{date}')")
                    if db_id is BigQuery:
                        pql.run_statement(
                            f"insert into {table}(id, v) values ({index}, cast(timestamp '{date}' as {date_type}))"
                        )
                    else:
                        pql.run_statement(f"insert into {table}(id, v) values ({index}, timestamp '{date}')")
            pql.commit()

            conn = connect_to_uri(conn_string)
            assert type(conn) is db_id  # Might change in the future

            if db_id is MySQL:
                conn.query("SET @@session.time_zone='+00:00'", None)

            for date_type, table in date_type_tables.items():
                if db_id is Snowflake:
                    table = table.upper()
                schema = conn.query_table_schema(table.split("."))
                schema = {k.lower(): v for k, v in schema.items()}
                try:
                    v_type = schema["v"]
                except KeyError:
                    raise AssertionError(f"Bad schema {schema} for table {table}, data type {date_type}, conn {conn}")
                v_type = v_type.replace(precision=precision)

                returned_dates = tuple(
                    x
                    for x, in conn.query(
                        Select([conn.normalize_value_by_type("v", v_type)], table, order_by=["id"]), list
                    )
                )

                # print("@@", db_id, date_type, " --> ", returned_dates)
                results.append((db_id, date_type, returned_dates))

        finally:
            if conn:
                conn.close()
            for t in used_tables:
                try:
                    pql.run_statement(f"DROP TABLE {t}")
                except preql.Signal:
                    pass

        return results

    def test_normalize(self):
        tpe = ThreadPoolExecutor()

        all_returned = {
            (db_id, date_type): returned_dates
            for gen in tpe.map(self._test_dates_for_db, CONN_STRINGS.items())
            for db_id, date_type, returned_dates in gen
        }

        all_reprs = set(all_returned.values())
        # for r in all_reprs:
        #     print('-', r)
        assert len(all_reprs) == 1
