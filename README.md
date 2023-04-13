<p align="center">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="50%" />
</p>

# **data-diff**

## What is `data-diff`?
data-diff is a **free, open-source tool** that enables data professionals to detect differences in values between any two tables.

## Documentation

[**🗎 Documentation**](https://docs.datafold.com/guides/os_data_diff) - our detailed documentation has everything you need to start diffing.

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

For their corresponding connection strings, check out our [detailed table](https://github.com/datafold/data-diff/blob/master/docs/supported-databases.md).

#### Looking for a database not on the list?
If a database is not on the list, we'd still love to support it. [Please open an issue](https://github.com/datafold/data-diff/issues) to discuss it, or vote on existing requests to push them up our todo list.

## Get started

### Installation

#### First, install `data-diff` using `pip`.

```
pip install data-diff
```

#### Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'data-diff[mysql]'`

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- `pip install 'data-diff[presto]'`

- `pip install 'data-diff[oracle]'`

- `pip install 'data-diff[trino]'`

- `pip install 'data-diff[clickhouse]'`

- `pip install 'data-diff[vertica]'`

- For BigQuery, see: https://pypi.org/project/google-cloud-bigquery/

_Some drivers have dependencies that cannot be installed using `pip` and still need to be installed manually._

### Run your first diff

Once you've installed `data-diff`, you can run it from the command line.

```
data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]
```

Be sure to read [the docs](https://docs.datafold.com/reference/open_source/cli) for detailed instructions how to build one of these commands depending on your database setup.

#### Code Example: Diff Tables Between Databases
Here's an example command for your copy/pasting, taken from the screenshot above when we diffed data between Snowflake and Postgres.

```
data-diff \
  postgresql://<username>:'<password>'@localhost:5432/<database> \
  <table> \
  "snowflake://<username>:<password>@<password>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>" \
  <TABLE> \
  -k activity_id \
  -c activity \
  -w "event_timestamp < '2022-10-10'"
```

#### Code Example: Diff Tables Within a Database

```
data-diff \
  "snowflake://<username>:<password>@<password>/<DATABASE>/<SCHEMA_1>?warehouse=<WAREHOUSE>&role=<ROLE>" <TABLE_1> \
  <SCHEMA_2>.<TABLE_2> \
  -k org_id \
  -c created_at -c is_internal \
  -w "org_id != 1 and org_id < 2000" \
  -m test_results_%t \
  --materialize-all-rows \
  --table-write-limit 10000
```

In both code examples, I've used `<>` carrots to represent values that **should be replaced with your values** in the database connection strings. For the flags (`-k`, `-c`, etc.), I opted for "real" values (`org_id`, `is_internal`) to give you a more realistic view of what your command will look like.

### We're here to help!

We're here to help! Please post any questions in [GitHub Discussions](https://github.com/datafold/data-diff/discussions).

## How to Use

* [Examples with dbt, joindiff, and hashdiff](https://docs.datafold.com/reference/open_source/cli#examples)
* [Examples with Python](https://data-diff.readthedocs.io/en/latest/python-api.html)
* [How to use with TOML configuration file](https://docs.datafold.com/reference/open_source/cli#toml-config-file)

## How to Contribute
* Feel free to open an issue or contribute to the project by working on an existing issue.
* Please read the [contributing guidelines](https://github.com/datafold/data-diff/blob/master/CONTRIBUTING.md) to get started.
* To add a new database driver, check out [docs](https://github.com/datafold/data-diff/blob/master/docs/new-database-driver-guide.rst).

Big thanks to everyone who contributed so far:

<a href="https://github.com/datafold/data-diff/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=datafold/data-diff" />
</a>

## Technical Explanation

Check out this [technical explanation](https://github.com/datafold/data-diff/blob/master/docs/technical-explanation.md) of how data-diff works.

## Analytics
* [Usage Analytics & Data Privacy](https://github.com/datafold/data-diff/blob/master/docs/usage_analytics.md)

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
