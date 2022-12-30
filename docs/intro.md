# Introduction to Sqeleton

Sqeleton is a Python library for querying SQL databases. 

It has two complementary APIs:

1. [Database interface](#database-interface), for connecting and querying.

2. [Query builder](#query-builder), for constructing query expressions (ASTs).

The following sections delve into each one of them.

## Database Interface

Sqeleton's database interface enables provides a unified interface for accessing a wide array of databases.

Each database has its own dialect, which tells the compiler which SQL to produce for each expression.  Extra features can be added to the dialect using [mixins](#database-mixins).

### connect()

Database instances are created using the `connect()` function, when given a URI or equivalent dict:

```python
>>> from sqeleton import connect
>>> connect("mysql://localhost/db")
<sqeleton.databases.mysql.MySQL object at ...>
>>> connect({"driver": "mysql", "host": "localhost", "database": "db"})
<sqeleton.databases.mysql.MySQL object at ...>
```

For non-cloud DBs, you can also provide the size of the local thread pool, i.e. how many worker-threads it should spawn.

Database instances have a few useful methods, like `list_tables()`, `query_table_schema()`:

```python
>>> db = connect("postgresql:///")
>>> db.name
'PostgreSQL'
>>> db.list_tables('R%') 
[('Rating',), ('Rating_del1',), ('Rating_del1p',), ('Rating_update001p',), ('Rating_update1',), ('Rating_update1p',), ('Rating_update50p',)]
>>> db.close()  
>>> db.is_closed
True
```

### Database.query()

The query function accepts either raw SQL, or a query expression, created by the query-builder:

```python
>>> db = connect("duckdb://:memory:")

# Query raw SQL
>>> db.query('select 1 + 1') 
[(2,)]

# Query SQL expression (more on that later)
>>> from sqeleton.queries import current_timestamp
>>> db.query(current_timestamp())
datetime.datetime(2022, 12, 28, 17, 1, 11)
```

It also accepts a list of statements:

```python
from sqeleton.queries import table
>>> tmp = table('tmp', schema={'i': int})
# Query list of statements, returns the last result
>>> db.query([
...     tmp.create(),
...     tmp.insert_rows([x] for x in range(100)),
...     'select sum(i) from tmp',
... ])
[(4950,)]
```

It's possible to tell the `query()` method the type you expect as a result, and it will validate and attempt to cast it:

```python
# Same as default. Return a list of tuples.
>>> db.query('select 1 + 1', list)   
[(2,)]
# Return a tuple. Expects only 1 row returned.
>>> db.query('select 1 + 1', tuple)  
(2,)
# Return an int. Expects only 1 row and column returned.
>>> db.query('select 1 + 1', int) 
2
```

## Query Builder

Sqeleton's query-builder takes a lot of ideas from both SQLAlchemy and PyPika, however there are several notable differences.

### table()

To query tables, users create a table instance, and use chain methods to build the query expression:

```python
>>> from sqeleton import table

# Query everything in table 'tmp'
>>> expr = table('tmp').select() 
>>> db.query(expr)
[(0,), (1,), (2,), (3,), ...]
```

To specify a dotted table path (for schemas, etc.), use either form:

- `table('foo', 'bar')` compiles to `foo.bar`.
- `table(('foo', 'bar'))` also compiles to `foo.bar`.

### table attributes and `this`

In sqeleton, table attributes are accessed either through the table's `[]` operator, or using the `this` object. When compiled, `this.foo` evaluates to the column of the attached table:

```python
from sqeleton import table, this

tbl = table('tmp')

# All exprs are equivalent to: SELECT foo, bar FROM tmp
expr1 = tbl.select(tbl['foo'], tbl['bar'])
expr1 = tbl.select(this['foo'], this['bar'])
expr2 = tbl.select(this.foo, this.bar) 
```

It's also possible to give `this` a list of attributes:

```python
attrs = ['foo', 'bar']

# Equivalent to: SELECT foo, bar FROM tbl
expr = tbl.select(*this[attrs])
```

It's recommended to prefer the `this.attr` syntax whenever possible, because it's shorter, more readable, and more amenable to refactoring.

However, the `[]` syntax is very useful for variable attribute names.


### .select(), .where(), .order_by(), .limit()

These are fairly straightforward, and similar to other libraries:

```python
# SELECT a FROM tmp WHERE b > 10 ORDER BY c LIMIT 20
table('tmp').select(this.a).where(this.b > 10).order_by(this.c).limit(20)
```

There are a few things worth mentioning:

- **SKIP** - You can provide the `sqeleton.SKIP` keyword to these functions, which does nothing. That can be useful for chaining conditionals. For example:

```python
from sqeleton import SKIP

# SELECT name[, age] FROM tmp [WHERE age > 18] [LIMIT limit]
rows = (table('tmp')
        .select(this.name, (this.age if include_age else SKIP))
        .where(this.age > 18 if only_adults else SKIP)
        .limit(limit if limit is not None else SKIP)
       )
```

- **Generators**

It's possible to provide generators to `select()`, `where()`, and `order_by()`, enabling syntax like this:

```python
fields = 'foo', 'bar', '_xyz'

# SELECT foo, bar FROM tmp WHERE foo > 0 AND bar > 0 AND _xyz > 0
all_above_0 = (table('tmp')
        .select(this[f] for f in fields if not f.startswith('_'))
        .where(this[f] > 0 for f in fields)
       )
```

### .group_by(), .agg(), and .having()

Group-by in sqeleton behaves like in SQL, except for a small change in syntax:

```python
# SELECT a, sum(b) FROM tmp GROUP BY 1
table('tmp').group_by(this.a).agg(this.b.sum())

# SELECT a, sum(b) FROM a GROUP BY 1 HAVING (b > 10)
(table('tmp')
    .group_by(this.a)
    .agg(this.b.sum())
    .having(this.b > 10)
)
```

These functions also accept generators and `SKIP`.

A call to `.agg()` must follow every call to `group_by()`. 

Any use of `.select()` will be considered a separate sub-query:

```python
# SELECT (c + 1) FROM (SELECT b, c FROM (SELECT a FROM tmp) GROUP BY 1)
rows = (table('tmp')
        .select(this.a)
        .group_by(this.b)
        .agg(this.c)
        .select(this.c + 1)
    )
```

### More table operations

Tables and queries also support the following methods:

```python
# SELECT count() FROM a`
a.count()

# SELECT * FROM a UNION b
a.union(b)

# SELECT * FROM a UNION ALL b  -- concat tables
a.union_all(b)

# SELECT * FROM a EXCEPT b      -- or MINUS
a.minus(b)

# select * from a INTERSECT b
a.intersect(b)
```

### .join(), .on()

When joining, it's recommended to use explicit tables names, instead of `this`, in order to avoid potential name collisions.

```python
person = table('person')
city = table('city')

name_and_city = (
    person
    .join(city)
    .on(person['city_id'] == city['id'])
    .select(person['id'], city['name'])
)

```
`.on()` also supports generators and `SKIP`.


### `when()` and `.then()`

Sqeleton provides a way to construct case-when-then expressions:

```python
from sqeleton.queries import when

# SELECT CASE
#   WHEN (type = 'text') THEN text
#   WHEN (type = 'number') THEN number
#   ELSE 'unknown type' END
# FROM foo
rows = table('foo').select(
        when(this.type == 'text').then(this.text)
        .when(this.type == 'number').then(this.number)
        .else_('unknown type')
    )
```

Each `.when()` must be followed by a `.then()`.

Sqeleton also provides a convenience `if_()` function for simple conditionals:

```python
from sqeleton import if_

# SELECT CASE WHEN b THEN c ELSE d END FROM foo
table('foo').select(if_(b, c, d))

```


### DDL - .create(), .drop(), .truncate()

These methods create DDL expressions (or "data-definition language"). 

To execute these expressions on a database, you have to call `Database.query()`.

- **.create()**

It's possible to create empty new tables using a schema, or new tables populated using a query.

Both are done by using the `.create()` method:

```python
source_table = table('source')

db.query([

    # CREATE TABLE new AS SELECT * FROM source
    table('new').create(source_table),

    # CREATE TABLE new AS SELECT * FROM source WHERE x > 0
    table('new_nonzero').create(source_table.where(this.x > 0)),

    # CREATE TABLE foo (id INT, name VARCHAR)
    table('foo', schema={
        id: int,
        name: str
    }).create()

])
```

The `.create()` method also accepts the following keyword parameters:

- `if_not_exists` - Adds `IF NOT EXISTS` to the create statement

- `primary_keys` - Specify primary keys when creating the table

- **.drop()**, **.truncate()**

These are the simple parallels of `DROP TABLE` and `TRUNCATE TABLE`.


### Add data - .insert_row(), .insert_rows(), .insert_expr()

These methods insert rows of constant values (from Python), or from a query expression.

```python
# INSERT INTO atoms VALUES ('H', 1)
table('atoms').insert_row("H", 1)

# INSERT INTO atoms VALUES ('H', 1), ('He', 2)
rows = [
    ("H", 1),
    ("He", 2)
] 
table('atoms').insert_rows(rows)

# INSERT INTO foo SELECT * FROM bar     -- Concat 'bar' to 'foo'
table('foo').insert_expr(table('bar'))
```

A common pattern is to call `.insert_rows()` with a generator:

```python
rows = {
    1: "H",
    2: "He",
}
table('atoms').insert_rows((sym, num) for num, sym in rows.items())
```

### Raw SQL using `code()`

It's possible to combine Sqeleton's query expressions with raw SQL code.

It allows users to use features and syntax that Sqeleton doesn't yet support.

Keep in mind that the code is very unlikely to port to other databases, so if you need to support more than one database, keep your use of `code()` to a minimum, and use it behind abstracting functions.

```python
from sqeleton import code

# SELECT b, <x> FROM tmp WHERE <y>
table('tmp').select(this.b, code("<x>")).where(code("<y>"))
```

It's the user's responsibility to make sure the contents of the string given to `code()` are correct and safe for execution. 

Strings given to `code()` are actually templates, and can embed query expressions given as arguments:

 
```python
def tablesample(tbl, size):
    return code("{tbl} TABLESAMPLE BERNOULLI ({size})", tbl=tbl, size=size)

nonzero = table('points').where(this.x > 0, this.y > 0)

# SELECT * FROM points WHERE (x > 0) AND (y > 0) TABLESAMPLE BERNOULLI (10)
sample_expr = tablesample(nonzero)
```


### Bound Tables & Expressions - Database.table()

Sqeleton's query expressions are database-agnostic, which makes it easy to run the exact same queries on different databases.

While this functional style is sufficient for most purposes, it's sometimes convenient to have a more object-oriented approach, and pass around query expressions bound to a specific database. That can be especially useful when running all the queries on the same database, or when different databases need drastically different queries.

Bound exprs support `.query()`, which will execute them on the bound database:

```python
# SELECT foo FROM tmp
db.table('tmp').select(this.foo).query()

# SELECT foo FROM tmp  -- expects one row and one column
db.table('tmp').select(this.foo).query(int)
```
Having bound tables, specifically, allow to add the useful `.query_schema()` API:

```python
# CREATE TABLE a (b FLOAT)
>>> schema = {'b': float}
>>> db.table('a', schema=schema).create().query()

# Queries the database for the schema, and returns a new bound table instance
>>> t2 = db.table('a').query_schema()
>>> t2.schema
{'b': Float(precision=5)}
```


## Advanced topics

### Dialect Mixins

### Query params

### Query interpreter