.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   supported-databases
   how-to-use
   python-api
   technical-explanation
   new-database-driver-guide

Introduction
------------

**Data-diff** is a command-line tool and Python library to efficiently diff
rows across two different databases.

⇄  Verifies across many different databases (e.g. *PostgreSQL* -> *Snowflake*) !

🔍 Outputs diff of rows in detail

🚨 Simple CLI/API to create monitoring and alerts

🔥 Verify 25M+ rows in <10s, and 1B+ rows in ~5min.

♾️  Works for tables with 10s of billions of rows

For more information, `See our README <https://github.com/datafold/data-diff#readme>`_

How to install
--------------

Requires Python 3.7+ with pip.

::

    pip install data-diff

For installing with 3rd-party database connectors, use the following syntax:

::

    pip install "data-diff[db1,db2]"

    e.g.
    pip install "data-diff[mysql,postgresql]"

Supported connectors:

- mysql
- postgresql
- snowflake
- presto
- oracle
- trino
- clickhouse
- vertica



Resources
---------

- Users
    - Source code (git): `<https://github.com/datafold/data-diff>`_
    - :doc:`supported-databases`
    - :doc:`how-to-use`
    - :doc:`python-api`
    - :doc:`technical-explanation`
- Contributors
   - :doc:`new-database-driver-guide`
