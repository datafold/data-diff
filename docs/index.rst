.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   python-api

Introduction
------------

**Data-diff** is a command-line tool and Python library to efficiently diff
rows across two different databases.

⇄  Verifies across many different databases (e.g. *Postgres* -> *Snowflake*) !

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

or when you need extras like mysql and postgres:

::

    pip install "data-diff[mysql,pgsql]"


How to use from Python
----------------------

.. code-block:: python

    # Optional: Set logging to display the progress of the diff
    import logging
    logging.basicConfig(level=logging.INFO)

    from data_diff import connect_to_table, diff_tables

    table1 = connect_to_table("postgres:///", "table_name", "id")
    table2 = connect_to_table("mysql:///", "table_name", "id")

    for sign, columns in diff_tables(table1, table2):
        print(sign, columns)

    # Example output:
    + ('4775622148347', '2022-06-05 16:57:32.000000')
    - ('4775622312187', '2022-06-05 16:57:32.000000')
    - ('4777375432955', '2022-06-07 16:57:36.000000')


Resources
---------

- Git: `<https://github.com/datafold/data-diff>`_

-  Reference

   -  :doc:`python-api`

-  Tutorials

   -  TODO


