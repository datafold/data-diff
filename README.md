# **data-diff**

_data-diff is in shape to be run in production, but it is also under development._

----

- 🐞Bugs? 💡Issues? 
  - Please [open an issue](https://github.com/datafold/data-diff/issues/new/choose)!
- 💬 Prefer to chat live? 
  - Find us in [#tools-data-diff](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S) in the [Locally Optimistic Slack][slack] or
  - [Please reach out to the product team](https://calendly.com/jp-toor/customer-interview-oss) share any product feedback or feature requests!
- 💸💸 **Looking for paid contributors!** 💸💸
  - We're looking for developers with a deep understanding of databases and solid Python knowledge. [**Apply here!**](https://docs.google.com/forms/d/e/1FAIpQLScEa5tc9CM0uNsb3WigqRFq92OZENkThM04nIs7ZVl_bwsGMw/viewform)

----

**data-diff** enables data professionals to detect differences in values between any two tables. It's fast, easy to use, and reliable--even at massive scale.

<img width="454" alt="visual represntation of a diff" src="https://user-images.githubusercontent.com/1799931/194626900-81be9980-b81e-47ca-934c-8bcb6262dfae.png">

# Getting started

## Install `data-diff` and database-specific drivers

First, install `data-diff` using `pip`.

```pip install data-diff```

**Note:** Once you've installed Python 3.7+, it's most likely that `pip` and `pip3` can be used interchangeably.

Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'data-diff[mysql]'`

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- `pip install 'data-diff[presto]'`

- `pip install 'data-diff[oracle]'`

- `pip install 'data-diff[trino]'`

- `pip install 'data-diff[clickhouse]'`

- `pip install 'data-diff[vertica]'`

- For BigQuery, see: https://pypi.org/project/google-cloud-bigquery/

## Run your first diff

Once you've installed `data-diff`, you can run it from the command line:

`data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

We've included examples here for PostgreSQL and Snowflake. Additional database configurations and examples are available in the (docs TODO.)[link]

### Comparing the same table in Snowflake vs Postgres

Here's an example comparing two versions of a large table in two different databases. The code here has `<>` carrot 🥕 around variables in place of content that you will replace with your own information.

```
$ data-diff \
  "snowflake://<YOUR_USERNAME>:<your_snowflake_password>@<YOUR_ACCOUNT>/SNOWFLAKE_DB/<YOUR_DATABASE>?warehouse=<YOUR_WAREHOUSE>&role=<YOUR_ROLE>" <TABLE_1_NAME> \
  postgresql://<YOUR_USERNAME>:<your_postgres_password>@<your_hostname>:5432/<your_database_name> <table_2_name>  \
  -k <the_primary_key> \
  -c <column_to_compare_1> -c <column_to_compare_2> -c <column_to_compare_x>
```

And here's what the command looks like when you replace the carrots with real values and see the results:

```
$ data-diff \
  "snowflake://gleb:very_secr3t_one@fold83729/SNOWFLAKE_DB/ANALYTICS?warehouse=OUR_SMALL_WAREHOUSE&role=ANALYST" WEBSITE_EVENTS \
  postgresql://gleb:floatingdoorknobs@localhost:5432/ANALYTICS website_events  \
  -k event_id \
  -c company_name -c amount

[TODO run the code ]
```


### Comparing tables within a database

In this example, we'll run a similar command comparing two tables within Snowflake. This could help you out when reviewing a PR and comparing the development vs production version of a table.

```
$ data-diff \
  "snowflake://gleb:very_secr3t_one@fold83729/SNOWFLAKE_DB/ANALYTICS?warehouse=OUR_SMALL_WAREHOUSE&role=ANALYST" WEBSITE_EVENTS \
  "snowflake://gleb:very_secr3t_one@fold83729/SNOWFLAKE_DB/ANALYTICS_DEV?warehouse=OUR_SMALL_WAREHOUSE&role=ANALYST" WEBSITE_EVENTS \
  -k event_id \
  -c company_name -c amount

[TODO run the code ]
```

[TODO want to learn more? Dive into the Datafold Documentation (link).]

# Usage Analytics & Data Privacy

data-diff collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most. 
[TODO Read more about this and how to opt out in the documentation (link).]

# Details for Developers and Contributors

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

$ pip3 install poetry # Python dependency isolation tool
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
