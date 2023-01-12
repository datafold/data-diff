# How to use

## How to use from the shell (or: command-line)

Run the following command:

```bash
    # Same-DB diff, using outer join
    $ data-diff  DB  TABLE1  TABLE2  [options]

    # Cross-DB diff, using hashes
    $ data-diff  DB1  TABLE1  DB2  TABLE2  [options]
```

Where DB is either a database URL that's compatible with SQLAlchemy, or the name of a database specified in a configuration file.

We recommend using a configuration file, with the ``--conf`` switch, to keep the command simple and manageable.

For a list of example URLs, see [list of supported databases](supported-databases.md).

Note: Because URLs allow many special characters, and may collide with the syntax of your command-line,
it's recommended to surround them with quotes.

### Options

  - `--help` - Show help message and exit.
  - `-k` or `--key-columns` - Name of the primary key column. If none provided, default is 'id'.
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
  - `-j` or `--threads` - Number of worker threads to use per database. Default=1.
  - `-w`, `--where` - An additional 'where' expression to restrict the search space.
  - `--conf`, `--run` - Specify the run and configuration from a TOML file. (see below)
  - `--no-tracking` - data-diff sends home anonymous usage data. Use this to disable it.
  - `--bisection-threshold` - Minimal size of segment to be split. Smaller segments will be downloaded and compared locally.
  - `--bisection-factor` - Segments per iteration. When set to 2, it performs binary search.
  - `-m`, `--materialize` - Materialize the diff results into a new table in the database.
                            If a table exists by that name, it will be replaced.
                            Use `%t` in the name to place a timestamp.
                            Example: `-m test_mat_%t`
  - `--assume-unique-key` - Skip validating the uniqueness of the key column during joindiff, which is costly in non-cloud dbs.
  - `--sample-exclusive-rows` - Sample several rows that only appear in one of the tables, but not the other. Use with `-s`.
  - `--materialize-all-rows` -  Materialize every row, even if they are the same, instead of just the differing rows.
  - `--table-write-limit` - Maximum number of rows to write when creating materialized or sample tables, per thread. Default=1000.
  - `-a`, `--algorithm` `[auto|joindiff|hashdiff]` - Force algorithm choice



### How to use with a configuration file

Data-diff lets you load the configuration for a run from a TOML file.

**Reasons to use a configuration file:**

- Convenience: Set-up the parameters for diffs that need to run often

- Easier and more readable: You can define the database connection settings as config values, instead of in a URI.

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

Import the `data_diff` module, and use the following functions:

- `connect_to_table()` to connect to a specific table in the database

- `diff_tables()` to diff those tables


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

## Usage Analytics & Data Privacy

data-diff collects anonymous usage data to help our team improve the tool and to apply development efforts to where our users need them most.

We capture two events: one when the data-diff run starts, and one when it is finished. No user data or potentially sensitive information is or ever will be collected. The captured data is limited to:

- Operating System and Python version
- Types of databases used (postgresql, mysql, etc.)
- Sizes of tables diffed, run time, and diff row count (numbers only)
- Error message, if any, truncated to the first 20 characters.
- A persistent UUID to identify the session, stored in `~/.datadiff.toml`

If you do not wish to participate, the tracking can be easily disabled with one of the following methods:

* In the CLI, use the `--no-tracking` flag.
* In the config file, set `no_tracking = true` (for example, under `[run.default]`)
* If you're using the Python API:
```python
import data_diff
data_diff.disable_tracking()    # Call this first, before making any API calls
# Connect and diff your tables without any tracking
```
