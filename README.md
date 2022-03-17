# X-Diff

A cross-database, efficient diff between mostly-similar database tables.

Use cases:

- Quickly validate that a table was copied correctly

- Find changes between two versions of the same table

Current databases support:

- PostgreSQL

- MySQL

# How does it work?

Logarithmic search

# How to install

Requires Python 3.6+ with pip.

    pip install datafold-xdiff      # doesn't work yet

# How to use

Usage: `xdiff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

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