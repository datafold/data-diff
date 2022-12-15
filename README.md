# Sqeleton

**Under construction!**

Python library for querying SQL databases.

It consists of -

- A fast and concise query builder, inspired by PyPika and SQLAlchemy

- A modular database interface, with drivers for a long list of SQL databases.

### Databases we support

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


### Built for performance

- Multi-threaded by default - introduce ThreadLocalInterpreter

- No ORM - Nice for beginners, but encourages bad behavior

## Type-aware

Type validation when building expressions (and make sure columns exist)

Allows type introspection

# TODO

- Transactions

- Indexes

- Date/time expressions

- Window functions

## Possible plans for the future (not determined yet)

- Cache compilation of repetitive queries for even faster query-building

- Compile control flow, functions

- Define tables using type-annotated classes (SQLModel style)
