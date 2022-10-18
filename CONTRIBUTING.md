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

For questions, and non-technical discussions, see [discussions](/datafold/data-diff/discussions).

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
