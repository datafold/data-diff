# Sqeleton

**Under construction!**

Sqeleton is a Python library for querying SQL databases.

It consists of -

- A fast and concise query builder, inspired by PyPika and SQLAlchemy

- A modular database interface, with drivers for a long list of SQL databases.

It is comparable to other libraries such as SQLAlchemy or PyPika, in terms of API and intended audience. However there are several notable ways in which it is different.

## Overview

### Built for performance

- Multi-threaded by default -
    The same connection object can be used from multiple threads without any additional setup.

- No ORM
    ORMs are easy and familiar, but they encourage bad and slow code. Sqeleton is designed to push the compute to SQL.

- Fast query-builder
    Sqeleton's query-builder runs about 4 times faster than SQLAlchemy's.

### Type-aware

Sqeleton has a built-in feature to query the schemas of the databases it supports.

This feature can be also used to inform the query-builder, either as an alternative to defining the tables yourself, or to validate that your definitions match the actual schema.

The schema is used for validation when building expressions, making sure the names are correct, and that the data-types align.

(Still WIP)

### Multi-database access

Sqeleton is designed to work with several databases at the same time. Its API abstracts away as many implementation details as possible.

Databases we fully support:

- PostgreSQL >=10
- MySQL
- Snowflake
- BigQuery
- Redshift
- Oracle
- Presto
- Databricks
- Trino
- Clickhouse
- Vertica
- DuckDB >=0.6
- SQLite (coming soon)

## Documentation

[Read the docs!](https://sqeleton.readthedocs.io)

Or jump straight to the [introduction](https://sqeleton.readthedocs.io/en/latest/intro.html).

### Install

Install using pip:

```bash
pip install sqeleton
```

It is recommended to install the driver dependencies using pip's `[]` syntax:

```bash
pip install 'sqeleton[mysql, postgresql]'
```

Read more in [install / getting started.](https://sqeleton.readthedocs.io/en/latest/install.html)

### Basic usage

```python
from sqeleton import connect, table, this

# Create a new database connection
ddb = connect("duckdb://:memory:")

# Define a table with one int column
tbl = table('my_list', schema={'item': int})

# Make a bunch of queries
queries = [
    # Create table 'my_list'
    tbl.create(),

    # Insert 100 numbers
    tbl.insert_rows([x] for x in range(100)),

    # Get the sum of the numbers
    tbl.select(this.item.sum())
]
# Query in order, and return the last result as an int
result = ddb.query(queries, int)    

# Prints: Total sum of 0..100 = 4950
print(f"Total sum of 0..100 = {result}")
```


# TODO

- Transactions

- Indexes

- Date/time expressions

- Window functions

## Possible plans for the future (not determined yet)

- Cache the compilation of repetitive queries for even faster query-building

- Compile control flow, functions

- Define tables using type-annotated classes (SQLModel style)
