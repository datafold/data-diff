# **data-diff**

**data-diff is currently under heavy development, if you run into issues,
please file an issue and we'll help you out ASAP!**

**data-diff** is a command-line tool and Python library to efficiently validate
whether two different databases are in sync, e.g. Postgres and Redshift. The
common use-case is to verify replication between two datastores with e.g.
Fivetran, Debezium, .. and fix missing rows and/or alert on missing/incorrect
data so you know whether something is wrong before your customer.

**data-diff** doesn't pull down and compare every row, because it is extremely
slow. It _also_ doesn't just do a `count(*)`, which doesn't yield much
information--and is too slow to complete on large tables.

Instead, **data-diff** hits a happy medium between validation confidence and
performance. It can validate 100M+ rows across databases in 10s of seconds.

It moves the computation into the databases by splitting the table into segments
that are each checksummed for the columns you care about, e.g. `id` and
`updated_at`. If a segment has a different checksum, it further splits that
segment into smaller segments, binary searching its way to the rows that are
different:

```console
data-diff \
    mysql://mysql:Password1@127.0.0.1/mysql rating \
    mysql://mysql:Password1@127.0.0.1/mysql rating_del1 \
    --bisection-threshold 10000 \
    --bisection-factor 6 \
    --update-column timestamp \
    --verbose

[12:35:09] INFO - Diffing tables | segments: 6, bisection threshold: 10000.
[12:35:09] INFO - . Diffing segment 1/6, key-range: 1..166667
[12:35:09] INFO - . Diffing segment 2/6, key-range: 166667..333333
[12:35:09] INFO - . Diffing segment 3/6, key-range: 333333..499999
[12:35:09] INFO - . Diffing segment 4/6, key-range: 499999..666665
[12:35:10] INFO - . . Diffing segment 1/6, key-range: 499999..527776
[12:35:10] INFO - . . . Diffing segment 1/6, key-range: 499999..504628
[12:35:10] INFO - . . . Diff found 1 different rows.
[12:35:10] INFO - . . . Diffing segment 2/6, key-range: 504628..509257
[12:35:10] INFO - . . . Diffing segment 3/6, key-range: 509257..513886
[12:35:10] INFO - . . . Diffing segment 4/6, key-range: 513886..518515
[12:35:10] INFO - . . . Diffing segment 5/6, key-range: 518515..523144
[12:35:10] INFO - . . . Diffing segment 6/6, key-range: 523144..527776
[12:35:10] INFO - . . Diffing segment 2/6, key-range: 527776..555553
[12:35:10] INFO - . . Diffing segment 3/6, key-range: 555553..583330
[12:35:10] INFO - . . Diffing segment 4/6, key-range: 583330..611107
[12:35:10] INFO - . . Diffing segment 5/6, key-range: 611107..638884
[12:35:10] INFO - . . Diffing segment 6/6, key-range: 638884..666665
+ (500001, 1452897891)
[12:35:10] INFO - . Diffing segment 5/6, key-range: 666665..833331
[12:35:10] INFO - . Diffing segment 6/6, key-range: 833331..1000001
```

## Supported Databases

| Database      | Connection string                                                             | Time to check 25M rows | Status |
|---------------|-------------------------------------------------------------------------------|------------------------|--------|
| Postgres      | `postgres://user:password@hostname:5432/database`                             | 5s                     | 💚      |
| MySQL         | `mysql://user:password@hostname:5432/database`                                | 5s                     | 💚      |
| Snowflake     | `snowflake://user:password@account/warehouse?database=database&schema=schema` |                        | 💚      |
| Oracle        | `oracle://username:password@hostname/database`                                |                        | 💛      |
| BigQuery      | `bigquery:///`                                                                |                        | 💛      |
| Redshift      | `redshift://username:password@hostname:5439/database`                         |                        | 💛      |
| Presto        |                                                                               |                        | ⏳      |
| ElasticSearch |                                                                               |                        | 📝      |
| Databricks    |                                                                               |                        | 📝      |
| Planetscale   |                                                                               |                        | 📝      |
| Clickhouse    |                                                                               |                        | 📝      |
| Pinot         |                                                                               |                        | 📝      |
| Druid         |                                                                               |                        | 📝      |
| Kafka         |                                                                               |                        | 📝      |

* 💚: Implemented and thoroughly tested.
* 💛: Implemented, but not thoroughly tested yet.
* ⏳: Implementation in progress.
* 📝: Implementation planned. Contributions welcome.

If a database is not on the list, we'd still love to support it. Open an issue
to discuss it.

# How to install

Requires Python 3.7+ with pip.

```pip install data-diff```

or when you need extras like mysql and postgres

```pip install "data-diff[mysql,pgsql]"```

# How to use

Usage: `data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

Options:

  - `--help` - Show help message and exit.
  - `-k` or `--key-column` - Name of the primary key column
  - `-t` or `--update-column` - Name of updated_at/last_updated column
  - `-c` or `--columns` - List of names of extra columns to compare
  - `-l` or `--limit` - Maximum number of differences to find (limits maximum bandwidth and runtime)
  - `-s` or `--stats` - Print stats instead of a detailed diff
  - `-d` or `--debug` - Print debug info
  - `-v` or `--verbose` - Print extra info
  - `-i` or `--interactive` - Confirm queries, implies `--debug`
  - `--max-age` - Considers only rows younger than specified. See `--min-age`.
                  Useful for specifying replication lag.
  - `--min-age` - Considers only rows older than specified.
                  Example: `--min-age=5min` considers only rows from the last 5 minutes.
                  Valid units: `d, days, h, hours, min, minutes, mon, months, s, seconds, w, weeks, y, years`
  - `--bisection-factor` - Segments per iteration. When set to 2, it performs binary search.
  - `--bisection-threshold` - Minimal bisection threshold. i.e. maximum size of pages to diff locally.
  - `-j` or `--threads` - Number of worker threads to use per database. Default=1.

# Technical Explanation

Let's consider a scenario with an `orders` table with 1M rows. Fivetran is
replicating it contionously from Postgres to Snowflake:

```
┌─────────────┐                        ┌─────────────┐
│  Postgres   │                        │  Snowflake  │
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
│       Postgres       │              │      Snowflake       │
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
│       Postgres       │              │      Snowflake       │
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
│       Postgres       │              │      Snowflake       │
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

## Development Setup

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

```shell-session
$ poetry run python3 -m unittest
```

**4. Seed the Database(s)**

First, download the CSVs of seeding data:

```shell-session
$ curl https://datafold-public.s3.us-west-2.amazonaws.com/1m.csv -o dev/ratings.csv

# For a larger data-set (but takes 25x longer to import):
# - curl https://datafold-public.s3.us-west-2.amazonaws.com/25m.csv -o dev/ratings.csv
```

Now you can insert it into the testing database(s):

```shell-session
# It's optional to seed more than one to run data-diff(1) against.
$ preql -f dev/prepare_db.pql mysql://mysql:Password1@127.0.0.1:3306/mysql
$ preql -f dev/prepare_db.pql postgres://postgres:Password1@127.0.0.1:5432/postgres

# Cloud databases
$ preql -f dev/prepare_db.psq snowflake://<uri>
$ preql -f dev/prepare_db.psq mssql://<uri>
$ preql -f dev/prepare_db_bigquery.pql bigquery:///<project> # Bigquery has its own scripts
```

**5. Run **data-diff** against seeded database**

```bash
poetry run python3 -m data_diff postgres://user:password@host:db Rating mysql://user:password@host:db Rating_del1 -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0
```

# License

[MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE)
