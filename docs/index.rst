.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   python-api


**data-diff** is a command-line tool and Python library to efficiently diff
rows across two different databases.

⇄  Verifies across many different databases (e.g. Postgres -> Snowflake) !

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

    for different_row in diff_tables(table1, table2):
        plus_or_minus, columns = different_row
        print(plus_or_minus, columns)


Resources
---------

- Git: `<https://github.com/datafold/data-diff>`_

-  Reference

   -  :doc:`python-api`

-  Tutorials

   -  TODO


