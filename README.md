dblite
======

Simple query interface to SQL databases.

Supports SQLite and Postgres.

Full API documentation available at https://suurjaak.github.io/dblite.

- [Installation](#installation)
- [Usage](#usage)
- [Queries](#queries)
  - [Name quoting](#name-quoting)
- [Adapters and converters](#adapters-and-converters)
- [Row factories](#row-factories)
- [Object-relational mapping](#object-relational-mapping)
- [SQLite](#sqlite)
- [Postgres](#postgres)
- [API](#api)
- [Dependencies](#dependencies)


Installation
------------

```bash
pip install dblite
```


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
    tx.insert("test", val="will be rolled back automatically by Transaction")
```

Queries directly on the Database object use autocommit mode:
every action query gets committed immediately.

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
# All module-level queries use the very first database created
dblite.fetchone("sqlite_master")
# Access the second default Database by engine name
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
can be a mapping, or a sequence of key-value pairs:

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
dblite.fetchall("test", order={"id": False})
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

Table and column name strings are not quoted automatically. Names with whitespace
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

Table and column names that were given as data classes and class members,
*are* quoted automatically if their values need escaping,
see [name quoting in objects](#name-quoting-in-objects).


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


Row factories
-------------

A custom row factory can be specified, to return results as desired type instead of dictionaries.

```python
def kvfactory(cursor, row):  # Returns row as [(colname, value), ].
    return list(zip([c[0] for c in cursor.description], row))

dblite.init(":memory:")
dblite.register_row_factory(kvfactory)
dblite.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
for row in dblite.select("sqlite_master"):
    print(row)  # Prints [("type", "table"), ("name", "test"), ..]
```

Row factory can also be specified per Database:

```python
db = dblite.init(":memory:")
db.row_factory = lambda cursor, row: row
db.executescript("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
for row in db.select("sqlite_master"):
    print(row)  # Prints ("table", "test", ..)
```

Database row factory overrides the globally registered factory, if any.


Object-relational mapping
-------------------------

dblite uses dictionaries as rows by default, but can just as easily operate with
various types of data classes.

If data attributes have been declared as properties on the class,
the class properties can be used directly in dblite in place of column names,
e.g. for `ORDER BY` clause.

(Such data descriptor properties are automatically available for
`property` attributes, classes with `__slots__`, and namedtuples).

### Data classes

```python
schema = "CREATE TABLE devices (id INTEGER PRIMARY KEY, name TEXT)"
class Device(object):
    def __init__(self, id=None, name=None):
        self._id   = id
        self._name = name

    def get_id(self): return self._id
    def set_id(self, id): self._id = id
    id = property(get_id, set_id)

    def get_name(self): return self._name
    def set_name(self, name): self._name = name
    name = property(get_name, set_name)
Device.__name__ = "devices"  # cls.__name__ will be used as table name

dblite.init(":memory:").executescript(schema)

device = Device(name="lidar")
device.id = dblite.insert(Device, device)

device.name = "solid-state lidar"
dblite.update(Device, device, {Device.id: device.id})

device = dblite.fetchone(Device, Device.id, where=device)
print(device.name)  # Will be None as we only selected Device.id

for device in dblite.fetchall(Device, order=Device.name):
    print(device.id, device.name)
    dblite.delete(Device, device)
```

It is also possible to use very simple data classes with no declared properties.

```python
schema = "CREATE TABLE devices (id INTEGER PRIMARY KEY, name TEXT)"
class Device(object):
    def __init__(self, id=None, name=None):
        self.id   = id
        self.name = name
Device.__name__ = "devices"  # cls.__name__ will be used as table name

dblite.init(":memory:").executescript(schema)

device = Device(name="lidar")
device.id = dblite.insert(Device, device)

device.name = "solid-state lidar"
dblite.update(Device, device, id=device.id)

device = dblite.fetchone(Device, "id", where=device)
print(device.name)  # Will be None as we only selected Device.id

for device in dblite.fetchall(Device, order="name"):
    print(device.id, device.name)
    dblite.delete(Device, device)
```

### Classes with `__slots__`

```python
schema = "CREATE TABLE devices (id INTEGER PRIMARY KEY, name TEXT)"
class Device(object):
    __slots__ = ("id", "name")
    def __init__(self, id=None, name=None):
        self.id   = id
        self.name = name
Device.__name__ = "devices"  # cls.__name__ will be used as table name

dblite.init(":memory:").executescript(schema)

device = Device(name="lidar")
device.id = dblite.insert(Device, device)

device.name = "solid-state lidar"
dblite.update(Device, device, id=device.id)

device = dblite.fetchone(Device, Device.id, where=device)
print(device.name)  # Will be None as we only selected Device.id

for device in dblite.fetchall(Device, order=Device.name):
    print(device.id, device.name)
    dblite.delete(Device, device)
```

### collections.namedtuple

```python
schema = "CREATE TABLE devices (id INTEGER PRIMARY KEY, name TEXT)"
Device = collections.namedtuple("devices", ("id", "name"))

dblite.init(":memory:").executescript(schema)

device = Device(id=None, name="lidar")
device_id = dblite.insert(Device, device)

device = Device(id=device_id, name="solid-state lidar")
dblite.update(Device, device, {Device.id: device_id})

device = dblite.fetchone(Device, Device.id, where=device)
print(device.name)  # Will be None as we only selected Device.id

for device in dblite.fetchall(Device, order=Device.name):
    print(device.id, device.name)
    dblite.delete(Device, device)
```

### Name quoting with objects

dblite automatically quotes table and column names in queries when using objects as arguments.

```python
schema = 'CREATE TABLE "restaurant bookings" ("group" TEXT, "table" TEXT, "when" TIMESTAMP, "PATRON" BOOLEAN)'
Booking = collections.namedtuple("_", ("group", "table", "when", "patron"))
Booking.__name__ = "restaurant bookings"

dblite.init(":memory:").executescript(schema)

booking1 = Booking("Squirrel Charity", "Table 16", datetime.datetime(2022, 12, 30, 20, 30), False)
booking2 = Booking("The Three Henrys", "Table 23", datetime.datetime(2022, 12, 30, 19, 00), True)
dblite.insert(Booking, booking1)
dblite.insert(Booking, booking2)

for booking in dblite.fetchall(Booking, order=Booking.when):
    print(booking.when, booking.group, booking.table, booking.patron)
```

For more thorough examples on using objects, see [test/test_orm.py](test/test_orm.py).

In Postgres, schema definition is looked up from the database to ensure properly cased
names in queries, as cased names for Postgres tables and columns must use the declared form.

If there is no exact match for the Python name in database, falls back to lower-case name,
or if name is lower-case, falls back to cased name if database has a single matching cased name.


SQLite
------

SQLite connection parameter needs to be a valid path or a path-like object,
or the special `":memory:"` for transient in-memory database.

Connection flags default to `check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES`,
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
fetching and materializing rows in batches:

```python
dblite.init("host=localhost user=postgres dbname=bigdata")
with Transaction(lazy=True) as tx:  # Can only run a single query
    for i, row in enumerate(tx.select("some really huge table")):
        print("Processing row #%s" % i)

# Can also specify size of fetched batches (default is 2000 rows)
with Transaction(lazy=True, itersize=100) as tx:
    for i, row in enumerate(tx.select("some really huge table")):
        print("Processing row #%s" % i)
```

Note that `executescript()` in Postgres forces an internal reload of schema metadata,
allowing `insert()´ to return inserted primary key value for newly created tables,
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
| `dblite.register_row_factory()`         | registers function to produce query results as custom type
|                                         | |
| **dblite.Database**                     | |
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
| `Database.cursor`                       | database engine cursor object
| `Database.row_factory`                  | custom row factory, as `function(cursor, row tuple)`
|                                         | |
| **dblite.Transaction**                  | |
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
| `Transaction.cursor`                    | database engine cursor object
| `Transaction.database`                  | returns transaction `Database` instance


Dependencies
------------

- Python 3 or Python 2.7
- six (https://pypi.org/project/six)

If using Postgres:
- psycopg2 (https://pypi.org/project/psycopg2)
