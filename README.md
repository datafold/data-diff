# Data Diff

A cross-database, efficient diff using checksums between mostly-similar database
tables.

- Validate that a table was copied properly
- Be alerted before your customer finds out, or your report is wrong
- Validate that your replication mechnism is working correctly
- Find changes between two versions of the same table

It uses a bisection algorithm to efficiently check if e.g. a table is the same
between MySQL and Postgres, or Postgres and Snowflake, or MySQL and RDS!

```python
$ data-diff postgres:/// Original  postgres:/// Original_1diff  -v --bisection-factor=4
[16:55:19] INFO - Diffing tables of size 25000095 and 25000095 | segments: 4, bisection threshold: 1048576.
[16:55:36] INFO - Diffing segment 0/4 of size 8333364 and 8333364
[16:55:45] INFO - . Diffing segment 0/4 of size 2777787 and 2777787
[16:55:52] INFO - . . Diffing segment 0/4 of size 925928 and 925928
[16:55:54] INFO - . . . Diff found 2 different rows.
+ (20000, 942013020)
- (20000, 942013021)
[16:55:54] INFO - . . Diffing segment 1/4 of size 925929 and 925929
[16:55:55] INFO - . . Diffing segment 2/4 of size 925929 and 925929
[16:55:55] INFO - . . Diffing segment 3/4 of size 1 and 1
[16:55:56] INFO - . Diffing segment 1/4 of size 2777788 and 2777788
[16:55:58] INFO - . Diffing segment 2/4 of size 2777788 and 2777788
[16:55:59] INFO - . Diffing segment 3/4 of size 1 and 1
[16:56:00] INFO - Diffing segment 1/4 of size 8333365 and 8333365
[16:56:06] INFO - Diffing segment 2/4 of size 8333365 and 8333365
[16:56:11] INFO - Diffing segment 3/4 of size 1 and 1
[16:56:11] INFO - Duration: 53.51 seconds.
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
  - `-c` or `--columns` - List of names of extra columns to compare
  - `-l` or `--limit` - Maximum number of differences to find (limits maximum bandwidth and runtime)
  - `-s` or `--stats` - Print stats instead of a detailed diff
  - `-d` or `--debug` - Print debug info
  - `-v` or `--verbose` - Print extra info
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

    2. If their size is below the threshold, diff them locallly and print the results.

    3. Else:  (they are different and above the threshold)

        1. Select `n-1` rows (checkpoints) in table `A`, splitting it into `n` segments of similar size.

        2. Filter out checkpoints that don't exist in table `B`.

        3. Split both `A` and `B` into `m <= n` segments according to the mutual checkpoints. `m` must be at least 2.

        4. For each pair of segments `Ai` and `Bi` (where `0 <= i <= m`), recurse into step 1.

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
brew install mysql postgresql # MacOS dependencies for C bindings
poetry install
```

**2. Download CSV of Testing Data**

```shell-session
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip -d dev/
```

**3. Start Databases**

```shell-session
docker-compose up -d mysql postgres
```

**4. Run Unit Tests**

```shell-session
poetry run python3 -m unittest
```

**5. Seed the Database(s)**

If you're just testing, we recommend just setting up one database (e.g.
Postgres) to avoid incurring the long setup time repeatedly.

```shell-session
preql -f dev/prepare_db.pql postgres://postgres:Password1@127.0.0.1:5432/postgres
preql -f dev/prepare_db.pql mysql://mysql:Password1@127.0.0.1:3306/mysql
preql -f dev/prepare_db.psq snowflake://<uri>
preql -f dev/prepare_db.psq mssql://<uri>
preql -f dev/prepare_db_bigquery.pql bigquery:///<project> # Bigquery has its own
```

**6. Run data-diff against seeded database**

```bash
poetry run python3 -m data_diff postgres://user:password@host:db Rating mysql://user:password@host:db Rating_del1 -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0
```

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
poetry install -E preql -E mysql -E pgsql -E snowflake
```

**2. Download CSV of Testing Data**

```shell-session
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip -d dev/
```

**3. Start Databases**

```shell-session
docker-compose up -d mysql postgres
```

**4. Run Unit Tests**

```shell-session
poetry run python3 -m unittest
```

**5. Seed the Database(s)**

If you're just testing, we recommend just setting up one database (e.g.
Postgres) to avoid incurring the long setup time repeatedly.

```shell-session
preql -f dev/prepare_db.pql postgres://postgres:Password1@127.0.0.1:5432/postgres
preql -f dev/prepare_db.pql mysql://mysql:Password1@127.0.0.1:3306/mysql
preql -f dev/prepare_db snowflake://<uri>
preql -f dev/prepare_db mssql://<uri>
preql -f dev/prepare_db_bigquery bigquery:///<project> # Bigquery has its own
```

**6. Run data-diff against seeded database**

```bash
poetry run python3 -m data_diff postgres://user:password@host:db Rating mysql://user:password@host:db Rating_del1 -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0
```

# License

[MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE)
