********************
Python API Reference
********************

.. py:module:: sqeleton


User API
========

.. autofunction:: table

.. autofunction:: code

.. autoclass:: connect
    :members:

.. autodata:: SKIP
.. autodata:: this

Database
--------

.. automodule:: sqeleton.databases.base
    :members:


Queries
--------

.. automodule:: sqeleton.queries.api
    :members:

Internals
=========

This section is for developers who wish to improve sqeleton, or to extend it within their own project.

Regular users might also find it useful for debugging and understanding, especially at this early stage of the project.


Query ASTs
-----------

.. automodule:: sqeleton.queries.ast_classes
    :members:

Query Compiler
--------------

.. automodule:: sqeleton.queries.compiler
    :members:

ABCS
-----

.. automodule:: sqeleton.abcs.database_types
    :members:

.. automodule:: sqeleton.abcs.mixins
    :members:

