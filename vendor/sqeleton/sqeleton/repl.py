import rich.table
import logging

# logging.basicConfig(level=logging.DEBUG)

from . import connect

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


def repl(uri):
    db = connect(uri)
    db_name = db.name

    while True:
        try:
            q = input(f"{db_name}> ").strip()
        except EOFError:
            return
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
                path = db.parse_table_name(table_name)
                print("->", path)
                schema = db.query_table_schema(path)
            except Exception as e:
                logging.error(e)
            else:
                print_table([(k, v[1]) for k, v in schema.items()], ["name", "type"], f"Table '{table_name}'")
        else:
            # Normal SQL query
            try:
                res = db.query(q)
            except Exception as e:
                logging.error(e)
            else:
                if res:
                    print_table(res.rows, res.columns, None)


def main():
    uri = sys.argv[1]
    return repl(uri)


if __name__ == "__main__":
    main()
