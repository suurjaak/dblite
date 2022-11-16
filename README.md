dblite
======

Simple query interface to SQL databases.

Supports SQLite and Postgres.


Usage
-----

```python
import dblite

# dblite.init("sqlite path" or {..postgres opts..})
dblite.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
dblite.insert("test", val=None)
for i in range(5):
    dblite.insert("test", {"val": i})
dblite.fetchone("test", id=1)
dblite.fetchall("test", order="val", limit=3)
dblite.update("test", {"val": "new"}, val=None)
dblite.fetchall("test", val=("IN", range(3)))
dblite.delete("test", id=5)
dblite.execute("DROP TABLE test")
```


Keyword arguments are added to `WHERE` clause, or to `VALUES` clause for `INSERT`:

```python
myid = dblite.insert("test", val="oh")
dblite.update("test", {"val": "ohyes"}, id=myid)
dblite.fetchone("test", val="ohyes")
dblite.delete("test", val="ohyes")
```


`WHERE` clause supports simple equality match, binary operators,
collection lookups (`"IN"`, `"NOT IN"`), raw SQL strings, or
arbitrary SQL expressions.

```python
dblite.fetchall("test", val="ciao")
dblite.fetchall("test", where={"id": ("<", 10)})
dblite.fetchall("test", id=("IN", range(5)))
dblite.fetchall("test", val=("IS NOT", None))
dblite.fetchall("test", where=[("LENGTH(val)", (">", 4)), ])
dblite.fetchall("test", where=[("EXPR", ("id = ? OR id > ? or id < ?", [0, 1, 2]))])
```


Argument for key-value parameters, like `WHERE` or `VALUES`,
can be a dict, or a sequence of key-value pairs:

```python
dblite.update("test", values={"val": "ohyes"}, where=[("id", 1)])
```


Argument for sequence parameters, like `GROUP BY`, `ORDER BY`, or `LIMIT`,
can be an iterable sequence like list or tuple, or a single value.

```python
  dblite.fetchall("test", group="val", order=["id", ("val", False)], limit=3)
```


Provides a simple context manager for transactions:

```python
with dblite.transaction() as tx:
    dblite.insert("test", val="will be rolled back")
    dblite.update("test", {"val": "will be rolled back"}, id=0)
    raise dblite.Rollback     # Rolls back uncommitted actions and exits block
    dblite.insert("test", val="this will never be reached")

with dblite.transaction(commit=False) as tx:
    dblite.insert("test", val="will be committed")
    tx.commit()           # Commits uncommitted actions
    dblite.insert("test", val="will be rolled back")
    tx.rollback()         # Rolls back uncommitted actions
    dblite.insert("test", val="will roll back automatically: no autocommit")
```


Module-level functions work on the first initialized connection, multiple databases
can be used by keeping a reference to the connection:

```python
db1 = dblite.init("file1.db", "CREATE TABLE foos (val text)")
db2 = dblite.init("file2.db", "CREATE TABLE bars (val text)")
db1.insert("foos", val="foo")
db2.insert("bars", val="bar")
```


Dependencies
------------

If using Postgres:
- psycopg2 (https://pypi.org/project/psycopg2)
