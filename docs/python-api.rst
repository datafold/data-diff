Python API Reference
====================

.. py:module:: data_diff

.. autofunction:: connect_to_uri

.. autoclass:: TableDiffer
    :members: __init__, diff_tables

.. autoclass:: TableSegment
    :members: __init__, get_values, choose_checkpoints, segment_by_checkpoints, count, count_and_checksum, is_bounded, new

.. autoclass:: data_diff.databases.database_types.AbstractDatabase
    :members:

.. autodata:: DbKey
.. autodata:: DbTime
.. autodata:: DbPath
