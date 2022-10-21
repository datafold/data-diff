<p align="center">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="50%" />
</p>

# **data-diff**

## What is `data-diff`?
data-diff is a **free, open-source tool** that enables data professionals to detect differences in values between any two tables. It's fast, easy to use, and reliable. Even at massive scale.

_Are you a developer with a deep understanding of databases and solid Python knowledge? [We're hiring!](https://www.datafold.com/careers)_

[**Check out our documentation!**](https://docs.datafold.com/os_diff/about)

## Use cases

### Diff Tables Between Databases
#### Quickly identify issues when moving data between databases

<p align="center">
  <img alt="diff2" src="https://user-images.githubusercontent.com/1799931/196754998-a88c0a52-8751-443d-b052-26c03d99d9e5.png" />
</p>

### Diff Tables Within a Database (available in pre release)
#### Improve code reviews by identifying data problems you don't have tests for
<p align="center">
  <a href=https://www.loom.com/share/682e4b7d74e84eb4824b983311f0a3b2 target="_blank">
    <img alt="Intro to Diff" src="https://user-images.githubusercontent.com/1799931/196576582-d3535395-12ef-40fd-bbbb-e205ccae1159.png" width="50%" height="50%" />
  </a>
</p>

&nbsp;
&nbsp;

## Get started

### Installation

#### First, install `data-diff` using `pip`.

```
pip install data-diff
```

To try out bleeding-edge features, including materialization of results in your data warehouse:

```
pip install data-diff --pre
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

Be sure to read [the How to Use section below](#how-to-use) which gets into specific details about how to build one of these commands depending on your database setup.

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

#### Code Example: Diff Tables Within a Database (available in pre-release)

Here's a code example from [the video](https://www.loom.com/share/682e4b7d74e84eb4824b983311f0a3b2), where we compare data between two Snowflake tables within one database.

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

We know, that in some cases, the data-diff command can become long and dense. And maybe you're new to the command line.

We're here to help [on slack](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S) if you have ANY questions as you use `data-diff` in your workflow.

## How to Use

[How to use from the shell (or: command-line)](https://data-diff.readthedocs.io/en/latest/how-to-use.html#how-to-use-from-the-shell-or-command-line)

[How to use from Python](https://data-diff.readthedocs.io/en/latest/how-to-use.html#how-to-use-from-python)

[Usage Analytics & Data Privacy](https://data-diff.readthedocs.io/en/latest/how-to-use.html#usage-analytics-data-privacy)


## Technical Explanation

See here: https://data-diff.readthedocs.io/en/latest/technical-explanation.html

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
