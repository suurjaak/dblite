dblite
======

Simple query interface to SQL databases.

Supports SQLite and Postgres.

- [Usage](#usage)
- [Queries](#queries)
  - [Name quoting](#name-quoting)
- [Adapters and converters](#adapters-and-converters)
- [SQLite](#sqlite)
- [Postgres](#postgres)
- [API](#api)
- [Dependencies](#dependencies)


Usage
-----

```python
import dblite

dblite.init(":memory:")  # Open SQLite in-memory database
dblite.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
dblite.insert("test", val=None)
for i in range(5):
    dblite.insert("test", {"val": i})
dblite.fetchone("test", id=1)  # Queries return dictionaries
dblite.fetchall("test", order="val", limit=3)
dblite.update("test", {"val": None}, val=2)
dblite.fetchall("test", val=("IN", range(3)))
dblite.delete("test", id=5)
dblite.executescript("DROP TABLE test")
```


Provides a simple context manager for transactions:

```python
# dblite.init("sqlite path" or {..postgres opts..})
with dblite.transaction() as tx:
    tx.insert("test", val="will be rolled back")
    tx.update("test", {"val": "will be rolled back"}, id=0)
    raise dblite.Rollback  # Rolls back uncommitted actions and exits block
    tx.insert("test", val="this will never be reached")
print("continuing, Rollback does not propagate out of managed context")

with dblite.transaction(commit=False) as tx:
    tx.insert("test", val="will be committed")
    tx.commit()  # Commits uncommitted actions
    tx.insert("test", val="will be rolled back")
    tx.rollback()  # Rolls back uncommitted actions
    tx.insert("test", val="will roll back automatically: no autocommit")
```

Queries directly on the Database object use autocommit mode.

Database instances are usable as context managers:

```python
with dblite.init("my.sqlite") as db:  # File will be closed on exiting block
    db.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
    db.insert("test", id=1, val="value")
```

The first Database instance created for engine is cached per engine,
consecutive `init()` calls with no connection options yield the cached instance.

```python
# Create default database for SQLite
dblite.init(":memory:")
# All module-level queries use the very first created
dblite.fetchall("sqlite_master")

# Create default database for Postgres
dblite.init("postgresql://user@localhost/mydb")
# All module-level queries use the very first created: SQLite
dblite.fetchone("sqlite_master")
# Access the second default Database
dblite.init(engine="postgres").fetchall("information_schema.columns")

# Grab references to either
db1 = dblite.init(engine="sqlite")
db2 = dblite.init(engine="postgres")
```


Queries
-------

Columns to `SELECT` can be a string, or a sequence of strings:

```python
# Result: SELECT *
dblite.fetchone("test")
dblite.fetchone("test", "*")

# Result: SELECT id
dblite.fetchone("test", "id")
dblite.fetchone("test", ["id"])

# Result: SELECT id, val
dblite.fetchone("test", "id, val")
dblite.fetchone("test", ["id", "val"])

# Can be arbitrary SQL expressions, invoking functions and assigning aliases
dblite.fetchone("test", "COUNT(*) AS total")
```


Keyword arguments are added to `WHERE` clause, or to `VALUES` clause for `INSERT`:

```python
myid = dblite.insert("test", val="lorem")
dblite.update("test", {"val": "lorem ipsum"}, id=myid)
dblite.fetchone("test", id=myid)
dblite.delete("test", val="lorem ipsum")
```


`WHERE` clause supports simple equality match, binary operators,
collection lookups (`"IN"`, `"NOT IN"`), raw SQL strings, or
arbitrary SQL expressions. Used SQL needs to be supported by the underlying engine.

```python
dblite.fetchall("test", val="ciao")
dblite.fetchall("test", where={"id": ("<", 10)})
dblite.fetchall("test", id=("IN", list(range(5))))
dblite.fetchall("test", val=("!=", None))
dblite.fetchall("test", val=("IS NOT", None))
dblite.fetchall("test", val=("LIKE", "%a%"))
dblite.fetchall("test", where=[("LENGTH(val)", (">", 4))])

dblite.fetchall("test", where=[("EXPR", ("LENGTH(val) > ?", [4]))])
dblite.fetchall("test", where=[("EXPR", ("val = ? OR id > ? or id < ?", [0, 1, 2]))])
```


`WHERE` arguments are `AND`-ed together, `OR` needs subexpressions:

```python
# Result: WHERE (id < 1 OR id > 2) AND val = 3
dblite.fetchall("test", where=[("id < ? OR id > ?", [1, 2]), ("val", 3)])
```


Argument for key-value parameters, like `WHERE` or `VALUES`,
can be a dict, or a sequence of key-value pairs:

```python
# Result: SET val = 'done' WHERE id = 1
dblite.update("test", values={"val": "done"}, where=[("id", 1)])
```


Argument for sequence parameters, like `GROUP BY`, `ORDER BY`, or `LIMIT`,
can be an iterable sequence like list or tuple, or a single value.

```python
# Result: SELECT * FROM test GROUP BY val
dblite.fetchall("test", group="val")
# Result: SELECT * FROM test GROUP BY id, val
dblite.fetchall("test", group="id, val")
dblite.fetchall("test", group=("id", "val"))
```

```python
# Result: SELECT * FROM test ORDER BY id
dblite.fetchall("test", order="id")
dblite.fetchall("test", order="id ASC")
dblite.fetchall("test", order=("id", False))
# Result: SELECT * FROM test ORDER BY id ASC val DESC
dblite.fetchall("test", order="id, val DESC")
dblite.fetchall("test", order=["id", ("val", True)])
dblite.fetchall("test", order=[("id", False), ("val", True)])
dblite.fetchall("test", order=[("id", "ASC"), ("val", "DESC")])
```

```python
# Result: SELECT * FROM test LIMIT 2 OFFSET 0
dblite.fetchall("test", limit=2)
dblite.fetchall("test", limit=(2, 0))
dblite.fetchall("test", limit=(2, -1))
dblite.fetchall("test", limit=(2, None))
# Result: SELECT * FROM test LIMIT 2 OFFSET 10
dblite.fetchall("test", limit=(2, 10))
# Result: SELECT * FROM test OFFSET 10
dblite.fetchall("test", limit=(-1, 10))
dblite.fetchall("test", limit=(None, 10))
```



### Name quoting

Table and column names are not quoted automatically. Names with whitespace
or non-alphanumeric characters or reserved words can be quoted with `Database.quote()`
and `Transaction.quote()`:

```python
with dblite.init("my.sqlite") as db:
    db.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, %s TEXT)" %
                     db.quote("my column"))
    db.insert("test", {"id": 1, db.quote("my column"): "value"})
    for row in db.select("test"):
        print(row["my column"])
```

Note that in Postgres, quoted identifiers are case-sensitive.


Adapters and converters
-----------------------

Provides options to register custom adapters and converters,
to auto-adapt Python types to database types in query parameters,
and to auto-convert database types to Python types in query results.

```python
dblite.init(":memory:")
dblite.register_adapter(json.dumps, (dict, list, tuple))
dblite.register_converter(json.loads, "JSON")

dblite.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, data JSON)")
dblite.insert("test", id=1, data={"some": {"nested": ["data", 1, 2]}})
dblite.fetchone("test")  # `data` is auto-converted to Python dictionary
```


SQLite
------

SQLite connection parameter needs to be a valid path or a path-like object,
or the special `":memory:"` for transient in-memory database.

Connections flags default to `check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES`,
can be overridden on init:

```python
dblite.init("/path/to/my.db", detect_types=False)
```

Note that SQLite connections do not support multiple concurrent isolated transactions,
transaction state is shared per connection. To mitigate this, Transaction contexts
in SQLite default to exclusive access:

```python
dblite.init(":memory:")
with dblite.transaction() as tx:
    print("Entering another Transaction with-block will block until this exits.")
```

This can be overridden for `SELECT`-only transactions:

```python
dblite.init(":memory:")
with dblite.transaction(exclusive=False) as tx:
    print("Will only be doing SELECT queries, no need for exclusion.")
    tx.fetchall("test")
```


Postgres
--------

Postgres connection parameters can be:
- Postgres URI scheme `"postgresql://user:pass@hostname:port/dbname?parameter1=val1&.."`
- Postgres keyword-value format `"user=myuser password=mypass host=myhost port=myport dbname=myname .."`
- dictionary of connection options `{"user": "myuser", "host": "myhost", ..}`

```python
# These are all equivalent:
dblite.init("postgresql://myuser@myhost/mydb")
dblite.init("user=myuser host=myhost dbname=mydb")
dblite.init({"user": "myuser", "host": "myhost", "dbname": "mydb"})
```

Postgres connection parameters can also be specified in OS environment,
via standard Postgres environment variables like `PGUSER` and `PGPASSWORD`.

By default uses a pool of 1..4 connections per Database.

```python
with dblite.init("host=localhost user=postgres dbname=mydb", maxconn=1):
    print("Use a pool of only 1 connection.")
with dblite.init("host=localhost user=postgres dbname=mydb", minconn=4, maxconn=8):
    print("Use a pool of 4..8 connections.")
```

Postgres transactions can specify database table schema name up front:

```python
dblite.init("host=localhost user=postgres dbname=mydb")
with dblite.transaction(schema="information_schema") as tx:
    for row in tx.fetchall("columns", table_schema="public",
                           order="table_name, dtd_identifier"):
        print(row["table_name"], row["column_name"], row["data_type"])
```

Postgres transactions support server-side cursors for iterative data access,
not fetching and materializing all rows at once:

```python
dblite.init("host=localhost user=postgres dbname=bigdata")
with Transaction(lazy=True) as tx:
    for i, row in enumerate(tx.select("some really huge table")):
        print("Processing row #%s" % i)
```

`executescript()` forces an internal reload of schema metadata,
allowing `insert()´ to return inserted primary key value,
and query parameters to be auto-cast to expected column types.


API
---

| Name                                    | Description
| --------------------------------------- | -----------------------------------------------------------------------------------------
| `dblite.init()`                         | returns a opened `dblite.Database` object, the first created if no options given
| `dblite.fetchall()`                     | runs `SELECT`, returns all rows
| `dblite.fetchone()`                     | runs `SELECT`, returns a single row, or `None`
| `dblite.insert()`                       | `INSERT` a single row into table, returns inserted ID
| `dblite.select()`                       | runs `SELECT`, returns cursor
| `dblite.update()`                       | `UPDATE` table, returns affected row count
| `dblite.delete()`                       | `DELETE` from table, returns affected row count
| `dblite.execute()`                      | executes SQL with arguments, returns cursor
| `dblite.executescript()`                | executes SQL as a script of one or more SQL statements
| `dblite.close()`                        | closes the database and all pending transactions, if open
| `dblite.transaction()`                  | returns `dblite.Transaction` context manager
| `dblite.register_adapter()`             | registers function to auto-adapt given Python types to database types in query parameters
| `dblite.register_converter()`           | registers function to auto-convert given database types to Python in query results
|                                         | |
| **dblite.Database**
| `Database.fetchall()`                   | runs `SELECT`, returns all rows
| `Database.fetchone()`                   | runs `SELECT`, returns a single row, or `None`
| `Database.insert()`                     | `INSERT` a single row into table, returns inserted ID
| `Database.select()`                     | runs `SELECT`, returns cursor
| `Database.update()`                     | `UPDATE` table, returns affected row count
| `Database.delete()`                     | `DELETE` from table, returns affected row count
| `Database.execute()`                    | executes SQL with arguments, returns cursor
| `Database.executescript()`              | executes SQL as a script of one or more SQL statements
| `Database.transaction()`                | returns `dblite.Transaction` context manager
| `Database.open()`                       | opens database connection if not already open
| `Database.close()`                      | closes the database and all pending transactions, if open
| `Database.closed`                       | whether database is not open
|                                         | |
| **dblite.Transaction**
| `Transaction.fetchall()`                | runs `SELECT`, returns all rows
| `Transaction.fetchone()`                | runs `SELECT`, returns a single row, or `None`
| `Transaction.insert()`                  | `INSERT` a single row into table, returns inserted ID
| `Transaction.select()`                  | runs `SELECT`, returns cursor
| `Transaction.update()`                  | `UPDATE` table, returns affected row count
| `Transaction.delete()`                  | `DELETE` from table, returns affected row count
| `Transaction.execute()`                 | executes SQL with arguments, returns cursor
| `Transaction.executescript()`           | executes SQL as a script of one or more SQL statements
| `Transaction.commit()`                  | commits pending actions, if any
| `Transaction.rollback()`                | rolls back pending actions, if any
| `Transaction.close()`                   | closes the transaction, performing commit or rollback as specified
| `Transaction.closed`                    | whether transaction is not open
| `Transaction.database`                  | returns transaction `Database` instance



Dependencies
------------

- Python 3 or Python 2.7
- six (https://pypi.org/project/six)

If using Postgres:
- psycopg2 (https://pypi.org/project/psycopg2)
