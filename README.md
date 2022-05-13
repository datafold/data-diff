# Data Diff

**`data-diff` is currently under heavy development, if you run into issues,
please file an issue and we'll help you out ASAP!**

A cross-database, efficient diff using checksums between mostly-similar database
tables.

- Validate that a table was copied properly
- Be alerted before your customer finds out, or your report is wrong
- Validate that your replication mechnism is working correctly
- Find changes between two versions of the same table

It uses a bisection algorithm and checksums to efficiently check if e.g. a table
is the same between MySQL and Postgres, or Postgres and Snowflake, or MySQL and
RDS!

```python
$ data-diff postgres:/// Original  postgres:/// Original_1diff -t timestamp -v --bisection-factor=4
[16:57:36] INFO - Diffing tables of size 25000095 and 25000095 | segments: 4, bisection threshold: 1048576.
[16:58:03] INFO - Diffing segment 1/4 of size 8333364 and 8333364
[16:58:12] INFO - Diffing segment 2/4 of size 8333365 and 8333365
[16:58:29] INFO - . Diffing segment 1/4 of size 2777787 and 2777787
[16:58:32] INFO - . Diffing segment 2/4 of size 2777788 and 2777788
[16:58:45] INFO - . . Diffing segment 1/4 of size 925925 and 925925
[16:58:46] INFO - . . Diffing segment 2/4 of size 925929 and 925929
[16:58:48] INFO - . . . Diff found 2 different rows.
+ (12500048, 1268104625)
- (12500048, 1268104626)
[16:58:48] INFO - . . Diffing segment 3/4 of size 925929 and 925929
[16:58:49] INFO - . . Diffing segment 4/4 of size 5 and 5
[16:58:50] INFO - . Diffing segment 3/4 of size 2777788 and 2777788
[16:58:52] INFO - . Diffing segment 4/4 of size 2 and 2
[16:58:55] INFO - Diffing segment 3/4 of size 8333365 and 8333365
[16:59:00] INFO - Diffing segment 4/4 of size 1 and 1
[16:59:00] INFO - Duration: 89.92 seconds.
```

We currently support the following databases:

- PostgreSQL
- MySQL
- Oracle
- Snowflake
- BigQuery
- Redshift

We plan to add more, including NoSQL, and even APIs like Shopify!

# How to install

Requires Python 3.7+ with pip.

```pip install data-diff```

or when you need extras like mysql and postgres

```pip install "data-diff[mysql,pgsql]"```

# How to use

Usage: `data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

Options:

  - `--help` - Show help message and exit.
  - `-k` or `--key_column` - Name of the primary key column
  - `-t` or `--update-column` - Name of updated_at/last_updated column
  - `-c` or `--columns` - List of names of extra columns to compare
  - `-l` or `--limit` - Maximum number of differences to find (limits maximum bandwidth and runtime)
  - `-s` or `--stats` - Print stats instead of a detailed diff
  - `-d` or `--debug` - Print debug info
  - `-v` or `--verbose` - Print extra info
  - `-i` or `--interactive` - Confirm queries, implies `--debug`
  - `--min-age` - Considers only rows older than specified.
                  Example: `--min-age=5min` ignores rows from the last 5 minutes.
                  Valid units: `d, days, h, hours, min, minutes, mon, months, s, seconds, w, weeks, y, years`
  - `--max-age` - Considers only rows younger than specified.  See `--min-age`.
  - `--bisection-factor` - Segments per iteration. When set to 2, it performs binary search.
  - `--bisection-threshold` - Minimal bisection threshold. i.e. maximum size of pages to diff locally.


# How does it work?

Data Diff finds the differences between two tables by utilizing checksum calculations and logarithmic search.

Instead of comparing the entire table, it compares the tuple (primary_key, version_column), where the primary key is a unique identifier of the rows, and the version_column updates each time the row changes to a new value, that is unique to that update. Usually the versioning column would be a timestamp like `updated_at`, that would auto-update by the database. But it could also be an auto-counting integer, and so on.

Data Diff runs a checksum on these columns using MD5. If the checksums are not the same, we know the tables are different. We then split each table into "n" different segments of similar size (determined by the bisection factor), and repeat the comparison for each matching pair of segments. When segments are below a certain size (bisection threshold), we instead download the segments to the client, and diff them locally.

Data Diff splits the segments using "checkpoints", to ensure that inserted or deleted rows don't affect the quality of the diff.

This process is incremental, so differences are printed to stdout as they are found. Users can ensure Data Diff quits after finding some number of differences, either by providing the `--limit` option, or by closing the pipe (for example, by piping to `head`).

The algorithm goes like this:

0. Table segments `A` and `B` are set to the two tables for comparison.

1. Calculate the checksums on `A` and `B` using MD5.

    1. If they are the same, the tables are considered equal. Stop.

    2. If their size is below the threshold, diff them locally and print the results.

    3. Else:  (they are different and above the threshold)

        1. Select `n-1` rows (checkpoints) in table `A`, splitting it into `n` segments of similar size.

        2. Filter out checkpoints that don't exist in table `B`.

        3. Split both `A` and `B` into `m <= n` segments according to the mutual checkpoints. `m` must be at least 2.

        4. For each pair of segments `Ai` and `Bi` (where `1 <= i <= m`), recurse into step 1.

## Example

The following printout shows the diff of two tables, Original and Original_1diff, with 25 million rows each, and just 1 different row between them.

We ran it with a very low bisection factor, and with the verbose flag, to demonstrate how it works.

Note: It's usually much faster to use high bisection factors, especially when there are very few changes, like in this example.

## Tips for performance

It's highly recommended that all involved columns are indexed.

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
