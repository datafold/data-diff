# **data-diff**

**data-diff is in shape to be run in production, but also under development. If
you run into issues or bugs, please [open an issue](https://github.com/datafold/data-diff/issues/new/choose) and we'll help you out ASAP! You can
also find us in `#tools-data-diff` in the [Locally Optimistic Slack][slack].**

**We'd love to hear about your experience using data-diff, and learn more your use cases. [Reach out to product team share any product feedback or feature requests!](https://calendly.com/jp-toor/customer-interview-oss)**



**data-diff** is a command-line tool and Python library to efficiently diff
rows across two different databases.

* ⇄  Verifies across [many different databases][dbs] (e.g. PostgreSQL -> Snowflake)
* 🔍 Outputs [diff of rows](#example-command-and-output) in detail
* 🚨 Simple CLI/API to create monitoring and alerts
* 🔁 Bridges column types of different formats and levels of precision (e.g. Double ⇆ Float ⇆ Decimal)
* 🔥 Verify 25M+ rows in <10s, and 1B+ rows in ~5min.
* ♾️  Works for tables with 10s of billions of rows

**data-diff** splits the table into smaller segments, then checksums each
segment in both databases. When the checksums for a segment aren't equal, it
will further divide that segment into yet smaller segments, checksumming those
until it gets to the differing row(s). See [Technical Explanation][tech-explain] for more
details.

This approach has performance within an order of magnitude of `count(*)` when
there are few/no changes, but is able to output each differing row! By pushing
the compute into the databases, it's _much_ faster than querying for and
comparing every row.

![Performance for 100M rows](https://user-images.githubusercontent.com/97400/175182987-a3900d4e-c097-4732-a4e9-19a40fac8cdc.png)

**†:** The implementation for downloading all rows that `data-diff` and
`count(*)` is compared to is not optimal. It is a single Python multi-threaded
process. The performance is fairly driver-specific, e.g. PostgreSQL's performs 10x
better than MySQL.

## Table of Contents

- [**data-diff**](#data-diff)
  - [Table of Contents](#table-of-contents)
  - [Common use-cases](#common-use-cases)
  - [Example Command and Output](#example-command-and-output)
  - [Supported Databases](#supported-databases)
- [How to install](#how-to-install)
  - [Install drivers](#install-drivers)
- [How to use](#how-to-use)
  - [How to use from the command-line](#how-to-use-from-the-command-line)
  - [How to use from Python](#how-to-use-from-python)
- [Technical Explanation](#technical-explanation)
  - [Performance Considerations](#performance-considerations)
- [Anonymous Tracking](#anonymous-tracking)
- [Development Setup](#development-setup)
- [License](#license)

## Common use-cases

* **Verify data migrations.** Verify that all data was copied when doing a
  critical data migration. For example, migrating from Heroku PostgreSQL to Amazon RDS.
* **Verifying data pipelines.** Moving data from a relational database to a
  warehouse/data lake with Fivetran, Airbyte, Debezium, or some other pipeline.
* **Alerting and maintaining data integrity SLOs.** You can create and monitor
  your SLO of e.g. 99.999% data integrity, and alert your team when data is
  missing.
* **Debugging complex data pipelines.** When data gets lost in pipelines that
  may span a half-dozen systems, without verifying each intermediate datastore
  it's extremely difficult to track down where a row got lost.
* **Detecting hard deletes for an `updated_at`-based pipeline**. If you're
  copying data to your warehouse based on an `updated_at`-style column, then
  you'll miss hard-deletes that **data-diff** can find for you.
* **Make your replication self-healing.** You can use **data-diff** to
  self-heal by using the diff output to write/update rows in the target
  database.

## Example Command and Output

Below we run a comparison with the CLI for 25M rows in PostgreSQL where the
right-hand table is missing single row with `id=12500048`:

```
$ data-diff \
    postgresql://user:password@localhost/database rating \
    postgresql://user:password@localhost/database rating_del1 \
    --bisection-threshold 100000 \ # for readability, try default first
    --bisection-factor 6 \ # for readability, try default first
    --update-column timestamp \
    --verbose

    # Consider running with --interactive the first time.
    # Runs `EXPLAIN` for you to verify the queries are using indexes.
    # --interactive
[10:15:00] INFO - Diffing tables | segments: 6, bisection threshold: 100000.
[10:15:00] INFO - . Diffing segment 1/6, key-range: 1..4166683, size: 4166682
[10:15:03] INFO - . Diffing segment 2/6, key-range: 4166683..8333365, size: 4166682
[10:15:06] INFO - . Diffing segment 3/6, key-range: 8333365..12500047, size: 4166682
[10:15:09] INFO - . Diffing segment 4/6, key-range: 12500047..16666729, size: 4166682
[10:15:12] INFO - . . Diffing segment 1/6, key-range: 12500047..13194494, size: 694447
[10:15:13] INFO - . . . Diffing segment 1/6, key-range: 12500047..12615788, size: 115741
[10:15:13] INFO - . . . . Diffing segment 1/6, key-range: 12500047..12519337, size: 19290
[10:15:13] INFO - . . . . Diff found 1 different rows.
[10:15:13] INFO - . . . . Diffing segment 2/6, key-range: 12519337..12538627, size: 19290
[10:15:13] INFO - . . . . Diffing segment 3/6, key-range: 12538627..12557917, size: 19290
[10:15:13] INFO - . . . . Diffing segment 4/6, key-range: 12557917..12577207, size: 19290
[10:15:13] INFO - . . . . Diffing segment 5/6, key-range: 12577207..12596497, size: 19290
[10:15:13] INFO - . . . . Diffing segment 6/6, key-range: 12596497..12615788, size: 19291
[10:15:13] INFO - . . . Diffing segment 2/6, key-range: 12615788..12731529, size: 115741
[10:15:13] INFO - . . . Diffing segment 3/6, key-range: 12731529..12847270, size: 115741
[10:15:13] INFO - . . . Diffing segment 4/6, key-range: 12847270..12963011, size: 115741
[10:15:14] INFO - . . . Diffing segment 5/6, key-range: 12963011..13078752, size: 115741
[10:15:14] INFO - . . . Diffing segment 6/6, key-range: 13078752..13194494, size: 115742
[10:15:14] INFO - . . Diffing segment 2/6, key-range: 13194494..13888941, size: 694447
[10:15:14] INFO - . . Diffing segment 3/6, key-range: 13888941..14583388, size: 694447
[10:15:15] INFO - . . Diffing segment 4/6, key-range: 14583388..15277835, size: 694447
[10:15:15] INFO - . . Diffing segment 5/6, key-range: 15277835..15972282, size: 694447
[10:15:15] INFO - . . Diffing segment 6/6, key-range: 15972282..16666729, size: 694447
+ (12500048, 1268104625)
[10:15:16] INFO - . Diffing segment 5/6, key-range: 16666729..20833411, size: 4166682
[10:15:19] INFO - . Diffing segment 6/6, key-range: 20833411..25000096, size: 4166685
```

## Supported Databases

| Database      | Connection string                                                                                                                   | Status |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------|--------|
| PostgreSQL >=10    | `postgresql://<user>:<password>@<host>:5432/<database>`                                                                             |  💚    |
| MySQL         | `mysql://<user>:<password>@<hostname>:5432/<database>`                                                                              |  💚    |
| Snowflake     | `"snowflake://<user>[:<password>]@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<role>[&authenticator=externalbrowser]"` |  💚    |
| Oracle        | `oracle://<username>:<password>@<hostname>/database`                                                                                |  💛    |
| BigQuery      | `bigquery://<project>/<dataset>`                                                                                                    |  💛    |
| Redshift      | `redshift://<username>:<password>@<hostname>:5439/<database>`                                                                       |  💛    |
| Presto        | `presto://<username>:<password>@<hostname>:8080/<database>`                                                                         |  💛    |
| Databricks    | `databricks://<http_path>:<access_token>@<server_hostname>/<catalog>/<schema>`                                                      |  💛    |
| Trino         | `trino://<username>:<password>@<hostname>:8080/<database>`                                                                          |  💛    |
| Clickhouse    | `clickhouse://<username>:<password>@<hostname>:9000/<database>`                                                                     |  💛    |
| ElasticSearch |                                                                                                                                     |  📝    |
| Planetscale   |                                                                                                                                     |  📝    |
| Pinot         |                                                                                                                                     |  📝    |
| Druid         |                                                                                                                                     |  📝    |
| Kafka         |                                                                                                                                     |  📝    |

* 💚: Implemented and thoroughly tested.
* 💛: Implemented, but not thoroughly tested yet.
* ⏳: Implementation in progress.
* 📝: Implementation planned. Contributions welcome.

If a database is not on the list, we'd still love to support it. Open an issue
to discuss it.

Note: Because URLs allow many special characters, and may collide with the syntax of your command-line,
it's recommended to surround them with quotes. Alternatively, you may provide them in a TOML file via the `--config` option.


# How to install

Requires Python 3.7+ with pip.

```pip install data-diff```

## Install drivers

To connect to a database, we need to have its driver installed, in the form of a Python library.

While you may install them manually, we offer an easy way to install them along with data-diff<sup>*</sup>:

- `pip install 'data-diff[mysql]'`

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- `pip install 'data-diff[presto]'`

- `pip install 'data-diff[oracle]'`

- `pip install 'data-diff[trino]'`

- `pip install 'data-diff[clickhouse]'`

- For BigQuery, see: https://pypi.org/project/google-cloud-bigquery/


Users can also install several drivers at once:

```pip install 'data-diff[mysql,postgresql,snowflake]'```

_<sup>*</sup> Some drivers have dependencies that cannot be installed using `pip` and still need to be installed manually._


### Install Psycopg2

In order to run Postgresql, you'll need `psycopg2`. This Python package requires some additional dependencies described in their [documentation](https://www.psycopg.org/docs/install.html#build-prerequisites).
An easy solution is to install [psycopg2-binary](https://www.psycopg.org/docs/install.html#quick-install) by running:

```pip install psycopg2-binary```

Which comes with a pre-compiled binary and does not require additonal prerequisites. However, note that for production use it is adviced to use `psycopg2`.


# How to use

## How to use from the command-line

Usage: `data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

See the [example command](#example-command-and-output) and the [sample
connection strings](#supported-databases).

Note that for some databases, the arguments that you enter in the command line
may be case-sensitive. This is the case for the Snowflake schema and table names.

Options:

  - `--help` - Show help message and exit.
  - `-k` or `--key-column` - Name of the primary key column
  - `-t` or `--update-column` - Name of updated_at/last_updated column
  - `-c` or `--columns` - Names of extra columns to compare.  Can be used more than once in the same command.
                          Accepts a name or a pattern like in SQL.
                          Example: `-c col% -c another_col -c %foorb.r%`
  - `-l` or `--limit` - Maximum number of differences to find (limits maximum bandwidth and runtime)
  - `-s` or `--stats` - Print stats instead of a detailed diff
  - `-d` or `--debug` - Print debug info
  - `-v` or `--verbose` - Print extra info
  - `-i` or `--interactive` - Confirm queries, implies `--debug`
  - `--json` - Print JSONL output for machine readability
  - `--min-age` - Considers only rows older than specified. Useful for specifying replication lag.
                  Example: `--min-age=5min` ignores rows from the last 5 minutes.
                  Valid units: `d, days, h, hours, min, minutes, mon, months, s, seconds, w, weeks, y, years`
  - `--max-age` - Considers only rows younger than specified. See `--min-age`.
  - `--bisection-factor` - Segments per iteration. When set to 2, it performs binary search.
  - `--bisection-threshold` - Minimal bisection threshold. i.e. maximum size of pages to diff locally.
  - `-j` or `--threads` - Number of worker threads to use per database. Default=1.
  - `-w`, `--where` - An additional 'where' expression to restrict the search space.
  - `--conf`, `--run` - Specify the run and configuration from a TOML file. (see below)
  - `--no-tracking` - data-diff sends home anonymous usage data. Use this to disable it.


### How to use with a configuration file

Data-diff lets you load the configuration for a run from a TOML file.

Reasons to use a configuration file:

- Convenience - Set-up the parameters for diffs that need to run often

- Easier and more readable - you can define the database connection settings as config values, instead of in a URI.

- Gives you fine-grained control over the settings switches, without requiring any Python code.

Use `--conf` to specify that path to the configuration file. data-diff will load the settings from `run.default`, if it's defined.

Then you can, optionally, use `--run` to choose to load the settings of a specific run, and override the settings `run.default`. (all runs extend `run.default`, like inheritance).

Finally, CLI switches have the final say, and will override the settings defined by the configuration file, and the current run.

Example TOML file:

```toml
# Specify the connection params to the test database.
[database.test_postgresql]
driver = "postgresql"
user = "postgres"
password = "Password1"

# Specify the default run params
[run.default]
update_column = "timestamp"
verbose = true

# Specify params for a run 'test_diff'.
[run.test_diff]
verbose = false
# Source 1 ("left")
1.database = "test_postgresql"                      # Use options from database.test_postgresql
1.table = "rating"
# Source 2 ("right")
2.database = "postgresql://postgres:Password1@/"    # Use URI like in the CLI
2.table = "rating_del1"
```

In this example, running `data-diff --conf myconfig.toml --run test_diff` will compare between `rating` and `rating_del1`.
It will use the `timestamp` column as the update column, as specified in `run.default`. However, it won't be verbose, since that
flag is overwritten to `false`.

Running it with `data-diff --conf myconfig.toml --run test_diff -v` will set verbose back to `true`.


## How to use from Python

API reference: [https://data-diff.readthedocs.io/en/latest/](https://data-diff.readthedocs.io/en/latest/)

Example:

```python
# Optional: Set logging to display the progress of the diff
import logging
logging.basicConfig(level=logging.INFO)

from data_diff import connect_to_table, diff_tables

table1 = connect_to_table("postgresql:///", "table_name", "id")
table2 = connect_to_table("mysql:///", "table_name", "id")

for different_row in diff_tables(table1, table2):
    plus_or_minus, columns = different_row
    print(plus_or_minus, columns)
```

Run `help(diff_tables)` or [read the docs](https://data-diff.readthedocs.io/en/latest/) to learn about the different options.

# Technical Explanation

In this section we'll be doing a walk-through of exactly how **data-diff**
works, and how to tune `--bisection-factor` and `--bisection-threshold`.

Let's consider a scenario with an `orders` table with 1M rows. Fivetran is
replicating it contionously from PostgreSQL to Snowflake:

```
┌─────────────┐                        ┌─────────────┐
│ PostgreSQL  │                        │  Snowflake  │
├─────────────┤                        ├─────────────┤
│             │                        │             │
│             │                        │             │
│             │  ┌─────────────┐       │ table with  │
│ table with  ├──┤ replication ├──────▶│ ?maybe? all │
│lots of rows!│  └─────────────┘       │  the same   │
│             │                        │    rows.    │
│             │                        │             │
│             │                        │             │
│             │                        │             │
└─────────────┘                        └─────────────┘
```

In order to check whether the two tables are the same, **data-diff** splits
the table into `--bisection-factor=10` segments.

We also have to choose which columns we want to checksum. In our case, we care
about the primary key, `--key-column=id` and the update column
`--update-column=updated_at`. `updated_at` is updated every time the row is, and
we have an index on it.

**data-diff** starts by querying both databases for the `min(id)` and `max(id)`
of the table. Then it splits the table into `--bisection-factor=10` segments of
`1M/10 = 100K` keys each:

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│      id=1..100k      │              │      id=1..100k      │
├──────────────────────┤              ├──────────────────────┤
│    id=100k..200k     │              │    id=100k..200k     │
├──────────────────────┤              ├──────────────────────┤
│    id=200k..300k     ├─────────────▶│    id=200k..300k     │
├──────────────────────┤              ├──────────────────────┤
│    id=300k..400k     │              │    id=300k..400k     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│      900k..100k      │              │      900k..100k      │
└───────────────────▲──┘              └▲─────────────────────┘
                    ┃                  ┃
                    ┃                  ┃
                    ┃ checksum queries ┃
                    ┃                  ┃
                  ┌─┻──────────────────┻────┐
                  │        data-diff        │
                  └─────────────────────────┘
```

Now **data-diff** will start running `--threads=1` queries in parallel that
checksum each segment. The queries for checksumming each segment will look
something like this, depending on the database:

```sql
SELECT count(*),
    sum(cast(conv(substring(md5(concat(cast(id as char), cast(timestamp as char))), 18), 16, 10) as unsigned))
FROM `rating_del1`
WHERE (id >= 1) AND (id < 100000)
```

This keeps the amount of data that has to be transferred between the databases
to a minimum, making it very performant! Additionally, if you have an index on
`updated_at` (highly recommended) then the query will be fast as the database
only has to do a partial index scan between `id=1..100k`.

If you are not sure whether the queries are using an index, you can run it with
`--interactive`. This puts **data-diff** in interactive mode where it shows an
`EXPLAIN` before executing each query, requiring confirmation to proceed.

After running the checksum queries on both sides, we see that all segments
are the same except `id=100k..200k`:

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│    checksum=0102     │              │    checksum=0102     │
├──────────────────────┤   mismatch!  ├──────────────────────┤
│    checksum=ffff     ◀──────────────▶    checksum=aaab     │
├──────────────────────┤              ├──────────────────────┤
│    checksum=abab     │              │    checksum=abab     │
├──────────────────────┤              ├──────────────────────┤
│    checksum=f0f0     │              │    checksum=f0f0     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│    checksum=9494     │              │    checksum=9494     │
└──────────────────────┘              └──────────────────────┘
```

Now **data-diff** will do exactly as it just did for the _whole table_ for only
this segment: Split it into `--bisection-factor` segments.

However, this time, because each segment has `100k/10=10k` entries, which is
less than the `--bisection-threshold` it will pull down every row in the segment
and compare them in memory in **data-diff**.

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│    id=100k..110k     │              │    id=100k..110k     │
├──────────────────────┤              ├──────────────────────┤
│    id=110k..120k     │              │    id=110k..120k     │
├──────────────────────┤              ├──────────────────────┤
│    id=120k..130k     │              │    id=120k..130k     │
├──────────────────────┤              ├──────────────────────┤
│    id=130k..140k     │              │    id=130k..140k     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│      190k..200k      │              │      190k..200k      │
└──────────────────────┘              └──────────────────────┘
```

Finally **data-diff** will output the `(id, updated_at)` for each row that was different:

```
(122001, 1653672821)
```

If you pass `--stats` you'll see e.g. what % of rows were different.

## Performance Considerations

* Ensure that you have indexes on the columns you are comparing. Preferably a
  compound index. You can run with `--interactive` to see an `EXPLAIN` for the
  queries.
* Consider increasing the number of simultaneous threads executing
  queries per database with `--threads`. For databases that limit concurrency
  per query, e.g. PostgreSQL/MySQL, this can improve performance dramatically.
* If you are only interested in _whether_ something changed, pass `--limit 1`.
  This can be useful if changes are very rare. This is often faster than doing a
  `count(*)`, for the reason mentioned above.
* If the table is _very_ large, consider a larger `--bisection-factor`. Explained in
  the [technical explanation][tech-explain]. Otherwise you may run into timeouts.
* If there are a lot of changes, consider a larger `--bisection-threshold`.
  Explained in the [technical explanation][tech-explain].
* If there are very large gaps in your key column, e.g. 10s of millions of
  continuous rows missing, then **data-diff** may perform poorly doing lots of
  queries for ranges of rows that do not exist (see [technical
  explanation][tech-explain]). We have ideas on how to tackle this issue, which we have
  yet to implement. If you're experiencing this effect, please open an issue and we
  will prioritize it.
* The fewer columns you verify (passed with `--columns`), the faster
  **data-diff** will be. On one extreme you can verify every column, on the
  other you can verify _only_ `updated_at`, if you trust it enough. You can also
  _only_ verify `id` if you're interested in only presence, e.g. to detect
  missing hard deletes. You can do also do a hybrid where you verify
  `updated_at` and the most critical value, e.g a money value in `amount` but
  not verify a large serialized column like `json_settings`.
* We have ideas for making **data-diff** even faster that
  we haven't implemented yet: faster checksums by reducing type-casts
  and using a faster hash than MD5, dynamic adaptation of
  `bisection_factor`/`threads`/`bisection_threshold` (especially with large key
  gaps), and improvements to bypass Python/driver performance limitations when
  comparing huge amounts of rows locally (i.e. for very high `bisection_threshold` values).

# Usage Analytics

data-diff collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most.

We capture two events, one when the data-diff run starts and one when it is finished. No user data or potentially sensitive information is or ever will be collected. The captured data is limited to:

- Operating System and Python version

- Types of databases used (postgresql, mysql, etc.)

- Sizes of tables diffed, run time, and diff row count (numbers only)

- Error message, if any, truncated to the first 20 characters.

- A persistent UUID to indentify the session, stored in `~/.datadiff.toml`

If you do not wish to participate, the tracking can be easily disabled with one of the following methods:

* In the CLI, use the `--no-tracking` flag.

* In the config file, set `no_tracking = true` (for example, under `[run.default]`)

* If you're using the Python API:

```python
import data_diff
data_diff.disable_tracking()    # Call this first, before making any API calls

# Connect and diff your tables without any tracking
```


# Development Setup

The development setup centers around using `docker-compose` to boot up various
databases, and then inserting data into them.

For Mac for performance of Docker, we suggest enabling in the UI:

* Use new Virtualization Framework
* Enable VirtioFS accelerated directory sharing

**1. Install Data Diff**

When developing/debugging, it's recommended to install dependencies and run it
directly with `poetry` rather than go through the package.

```
$ brew install mysql postgresql # MacOS dependencies for C bindings
$ apt-get install libpq-dev libmysqlclient-dev # Debian dependencies

$ pip install poetry # Python dependency isolation tool
$ poetry install # Install dependencies
```
**2. Start Databases**

[Install **docker-compose**][docker-compose] if you haven't already.

```shell-session
$ docker-compose up -d mysql postgres # run mysql and postgres dbs in background
```

[docker-compose]: https://docs.docker.com/compose/install/

**3. Run Unit Tests**

There are more than 1000 tests for all the different type and database
combinations, so we recommend using `unittest-parallel` that's installed as a
development dependency.

```shell-session
$ poetry run unittest-parallel -j 16 #  run all tests
$ poetry run python -m unittest -k <test> #  run individual test
```

**4. Seed the Database(s) (optional)**

First, download the CSVs of seeding data:

```shell-session
$ curl https://datafold-public.s3.us-west-2.amazonaws.com/1m.csv -o dev/ratings.csv

# For a larger data-set (but takes 25x longer to import):
# - curl https://datafold-public.s3.us-west-2.amazonaws.com/25m.csv -o dev/ratings.csv
```

Now you can insert it into the testing database(s):

```shell-session
# It's optional to seed more than one to run data-diff(1) against.
$ poetry run preql -f dev/prepare_db.pql mysql://mysql:Password1@127.0.0.1:3306/mysql
$ poetry run preql -f dev/prepare_db.pql postgresql://postgres:Password1@127.0.0.1:5432/postgres

# Cloud databases
$ poetry run preql -f dev/prepare_db.pql snowflake://<uri>
$ poetry run preql -f dev/prepare_db.pql mssql://<uri>
$ poetry run preql -f dev/prepare_db.pql bigquery:///<project>
```

**5. Run **data-diff** against seeded database (optional)**

```bash
poetry run python3 -m data_diff postgresql://postgres:Password1@localhost/postgres rating postgresql://postgres:Password1@localhost/postgres rating_del1 --verbose
```

**6. Run benchmarks (optional)**

```shell-session
$ dev/benchmark.sh #  runs benchmarks and puts results in benchmark_<sha>.csv
$ poetry run python3 dev/graph.py #  create graphs from benchmark_*.csv files
```

You can adjust how many rows we benchmark with by passing `N_SAMPLES` to `dev/benchmark.sh`:

```shell-session
$ N_SAMPLES=100000000 dev/benchmark.sh #  100m which is our canonical target
```


# License

[MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE)

[dbs]: #supported-databases
[tech-explain]: #technical-explanation
[perf]: #performance-considerations
[slack]: https://locallyoptimistic.com/community/
