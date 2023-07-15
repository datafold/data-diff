# Contributing to data-diff

Contributions are very welcome! We'll be happy to help you in the process.

## What should I know before I get started?

Go through the README and the documentation, and make sure that you understand how data-diff works.

## How to contribute?

### Reporting bugs

Please report the bug with as many details as you can.

1. Include the exact command that you used. Make sure to run data-diff with the `-d` flag for debug output.
2. Provide the entire output of the command. (stdout, logs, exception)
3. If possible, show us how we could reproduce the bug. i.e. how to set up an environment in which it occurs.

(When pasting, always make sure to redact sensitive information, like passwords.)

If data-diff returns incorrect results, i.e. false-positive or false-negative, please also include the original values.

Before you report a bug, make sure it doesn't already exist.

See [issues](/datafold/data-diff/issues/).

### Suggesting Enhancements

We are always interested to hear about how we can make data-diff better!

If you'd like us to support a new database, you should open an issue for it, if there isn't one already. If it already exists, make sure to vote for it with a :thumbsup:, to help us priortize it.

The same goes for other technical requests, like missing features, or gaps in the documentation.

See [issues](/datafold/data-diff/issues/).

For questions, and non-technical discussions, see [discussions](https://github.com/datafold/data-diff/discussions).

### Contributing code

#### Code style

All code should be formatted with `black -l 120`.

When in doubt, use the existing code as a guideline, or ask.

#### Get started (setup)

To get started, first clone the repository. For example `git clone https://github.com/datafold/data-diff`.

Once inside, you can install the dependencies.

- Option 1: Run `poetry install` to install them in a virtual env. You can then run data-diff using `poetry run data-diff ...` .

- Option 2: Run `pip install -e .` to install them, and data-diff, in the global context.

At the bare minimum, you need MySQL to run the tests.

You can create a local MySQL instance using `docker-compose up mysql`. The URI for it will be `mysql://mysql:Password1@localhost/mysql`. If you're using a different server, make sure to update `TEST_MYSQL_CONN_STRING` in `tests/common.py`. For your convenience, we recommend creating `tests/local_settings.py`, and to override the value there.

You can also run a few servers at once. For example `docker-compose up mysql postgres presto`.

Make sure to update the appropriate `TEST_*_CONN_STRING`, so that it will be included in the tests.

#### Run the tests

You can run the tests with `unittest`.

When running against multiple databases, the tests can take a long while.

To save time, we recommend running them with `unittest-parallel`.

When debugging, we recommend using the `-f` flag, to stop on error. Also, use the `-k` flag to run only the individual test that you're trying to fix.

#### Implementing a new database.

New databases should be added as a new module in the `data-diff/databases/` folder.

If possible, please also add the database setup to `docker-compose.yml`, so that we can run and test it for ourselves. If you do, also update the CI (`ci.yml`).

Guide to implementing a new database driver: https://data-diff.readthedocs.io/en/latest/new-database-driver-guide.html

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

## VSCode Setup
To debug using the unit tests in VSCode, add the following files to a `.vscode` directory in the root of the repo

`launch.json`
```
{
    "version": "0.1.0",
    "configurations": [
        {
            "name": "Debug Unit Test",
            "type": "python",
            "request": "test",
            "justMyCode": true,
        }
    ]
}
```

`settings.json`
```
{
    "python.testing.unittestArgs": [
        "-v",
        "-s",
        "",
        "-p",
        "test_*.py"
    ],
    "python.testing.pytestEnabled": false,
    "python.testing.unittestEnabled": true,
}
```
You should see that the tests are now appearing in the test explorer view:

![asdf](/docs/debug_example.png)

This will allow you to run tests in the IDE, debug them, and hit breakpoints.

Note that some tests require that you have the docker containers mentioned above running in order to pass.
