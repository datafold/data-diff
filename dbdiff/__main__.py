import time
from itertools import islice

from .diff_tables import TableSegment, TableDiffer
from .database import connect_to_uri

import click

@click.command()
@click.argument('db1_uri')
@click.argument('table1_name')
@click.argument('db2_uri')
@click.argument('table2_name')
@click.option('-c', '--column', default='updated_at', help='Name of column to compare')
def main(db1_uri, table1_name, db2_uri, table2_name, column='updated_at', limit=None):
    db1 = connect_to_uri(db1_uri)
    db2 = connect_to_uri(db2_uri)

    start = time.time()

    table1 = TableSegment(db1, (table1_name,), column)
    table2 = TableSegment(db2, (table2_name,), column)

    differ = TableDiffer()
    diff_iter = differ.diff_tables(table1, table2)

    if limit:
        diff_iter = islice(diff_iter, limit)

    diff = list(diff_iter)

    # print(diff)
    print("Diff:", len(diff))
    print("Diff %s%%" % (100 * len(diff) / table1.count))

    end = time.time()

    print("Duration:", end-start)


if __name__ == '__main__':
    main()