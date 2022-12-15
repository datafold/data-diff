import rich.table
import logging

# logging.basicConfig(level=logging.DEBUG)

from . import connect
from .queries import table

import sys


def print_table(rows, schema, table_name=""):
    # Print rows in a rich table
    t = rich.table.Table(title=table_name, caption=f"{len(rows)} rows")
    for col in schema:
        t.add_column(col)
    for r in rows:
        t.add_row(*map(str, r))
    rich.print(t)


def help():
    rich.print("Commands:")
    rich.print("  ?mytable - shows schema of table 'mytable'")
    rich.print("  * - shows list of all tables")
    rich.print("  *pattern - shows list of all tables with name like pattern")
    rich.print("Otherwise, runs regular SQL query")


def main():
    uri = sys.argv[1]
    db = connect(uri)
    db_name = db.name

    while True:
        q = input(f"{db_name}> ").strip()
        if not q:
            continue
        if q.startswith("*"):
            pattern = q[1:]
            names = db.query(db.dialect.list_tables(db.default_schema, like=f"%{pattern}%" if pattern else None))
            print_table(names, ["name"], "List of tables")
        elif q.startswith("?"):
            table_name = q[1:]
            if not table_name:
                help()
                continue
            try:
                schema = db.query_table_schema((table_name,))
            except Exception as e:
                logging.error(e)
            else:
                print_table([(k, v[1]) for k, v in schema.items()], ["name", "type"], f"Table '{table_name}'")
        else:
            try:
                res = db.query(q)
            except Exception as e:
                logging.error(e)
            else:
                if res:
                    print_table(res, [str(i) for i in range(len(res[0]))], q)


if __name__ == "__main__":
    main()
