.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   supported-databases
   python-api
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


How to use from the shell
-------------------------

.. code-block:: bash

    # Same-DB diff, using outer join
    $ data-diff  DB  TABLE1  TABLE2  [options]

    # Cross-DB diff, using hashes
    $ data-diff  DB1  TABLE1  DB2  TABLE2  [options]

We recommend using a configuration file, with the ``--conf`` switch, to keep the command simple and managable.


How to use from Python
----------------------

.. code-block:: python

    # Optional: Set logging to display the progress of the diff
    import logging
    logging.basicConfig(level=logging.INFO)

    from data_diff import connect_to_table, diff_tables

    table1 = connect_to_table("postgresql:///", "table_name", "id")
    table2 = connect_to_table("mysql:///", "table_name", "id")

    for sign, columns in diff_tables(table1, table2):
        print(sign, columns)

    # Example output:
    + ('4775622148347', '2022-06-05 16:57:32.000000')
    - ('4775622312187', '2022-06-05 16:57:32.000000')
    - ('4777375432955', '2022-06-07 16:57:36.000000')


Resources
---------

- Users
    - Source code (git): `<https://github.com/datafold/data-diff>`_
    - :doc:`supported-databases`
    - :doc:`python-api`
- Contributors
   - :doc:`new-database-driver-guide`
