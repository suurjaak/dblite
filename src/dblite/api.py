# -*- coding: utf-8 -*-
"""
Simple convenience wrapper for database operations.

    # db.init("sqlite path" or {..postgres opts..})
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
    db.insert("test", val=None)
    for i in range(5): db.insert("test", {"val": i})
    db.fetchone("test", id=1)
    db.fetchall("test", order="val", limit=3)
    db.update("test", {"val": "new"}, val=None)
    db.fetchall("test", val=("IN", range(3)))
    db.delete("test", id=5)
    db.execute("DROP TABLE test")


Keyword arguments are added to WHERE clause, or to VALUES clause for INSERT:

    myid = db.insert("test", val="oh")
    db.update("test", {"val": "ohyes"}, id=myid)
    db.fetchone("test", val="ohyes")
    db.delete("test", val="ohyes")


WHERE clause supports simple equality match, binary operators,
collection lookups ("IN", "NOT IN"), raw SQL strings, or
arbitrary SQL expressions.

    db.fetchall("test", val="ciao")
    db.fetchall("test", where={"id": ("<", 10)})
    db.fetchall("test", id=("IN", range(5)))
    db.fetchall("test", val=("IS NOT", None))
    db.fetchall("test", where=[("LENGTH(val)", (">", 4)), ])
    db.fetchall("test", where=[("EXPR", ("id = ? OR id > ? or id < ?", [0, 1, 2]))])


Function argument for key-value parameters, like WHERE or VALUES,
can be a dict, or a sequence of key-value pairs:

    db.update("test", values={"val": "ohyes"}, where=[("id", 1)])


Function argument for sequence parameters, like GROUP BY, ORDER BY, or LIMIT,
can be an iterable sequence like list or tuple, or a single value.

    db.fetchall("test", group="val", order=["id", ("val", False)], limit=3)


Provides a simple context manager for transactions:

    with db.transaction() as tx:
        db.insert("test", val="will be rolled back")
        db.update("test", {"val": "will be rolled back"}, id=0)
        raise db.Rollback     # Rolls back uncommitted actions and exits
        db.insert("test", val="this will never be reached")

    with db.transaction(commit=False) as tx:
        db.insert("test", val="will be committed")
        tx.commit()           # Commits uncommitted actions
        db.insert("test", val="will be rolled back")
        tx.rollback()         # Rolls back uncommitted actions
        db.insert("test", val="will roll back automatically: no autocommit")


Module-level functions work on the first initialized connection, multiple databases
can be used by keeping a reference to the connection:

    d1 = db.init("file1.db", "CREATE TABLE foos (val text)")
    d2 = db.init("file2.db", "CREATE TABLE bars (val text)")
    d1.insert("foos", val="foo")
    d2.insert("bars", val="bar")


------------------------------------------------------------------------------
This file is part of dblite - simple query interface to SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     05.03.2014
@modified    18.11.2022
------------------------------------------------------------------------------
"""
import collections
import datetime
import decimal
import glob
import importlib
import json
import logging
import os
import re

import six

logger = logging.getLogger(__name__)


def init(opts=None, engine=None, **kwargs):
    """
    Returns a Database object, creating one if not already open with these opts.
    If opts is None, returns the default database - the very first initialized.
    Module level functions use the default database.

    @param   opts    database connection options, engine-specific;
                     SQLite takes a file path or path-like object or `":memory:"`,
                     Postgres takes a Postgres URI scheme like `"postgresql://user@localhost/mydb"`
                     or a Postgres keyword=value format string like
                     `"host=localhost username=user dbname=mydb"`
                     or a dictionary of connection options like `dict(host="localhost", dbname=..)`
    @param   engine  database engine if not auto-detecting from connection options,
                     "sqlite" for SQLite3 and "postgres" for PostgreSQL (case-insensitive)
    @param   kwargs  additional arguments given to engine constructor,
                     e.g. `detect_types=sqlite3.PARSE_COLNAMES` for SQLite,
                     or `minconn=1, maxconn=4` for Postgres connection pool
    """
    return Database.factory(opts, engine, **kwargs)



def fetchall(table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
    """
    Convenience wrapper for database SELECT and fetch all.
    Keyword arguments are added to WHERE.
    """
    return init().fetchall(table, cols, where, group, order, limit, **kwargs)


def fetchone(table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
    """
    Convenience wrapper for database SELECT and fetch one.
    Keyword arguments are added to WHERE.
    """
    return init().fetchone(table, cols, where, group, order, limit, **kwargs)


def insert(table, values=(), **kwargs):
    """
    Convenience wrapper for database INSERT, returns inserted row ID.
    Keyword arguments are added to VALUES.
    """
    return init().insert(table, values, **kwargs)


def select(table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
    """
    Convenience wrapper for database SELECT, returns sqlite3.Cursor.
    Keyword arguments are added to WHERE.
    """
    return init().select(table, cols, where, group, order, limit, **kwargs)


def update(table, values, where=(), **kwargs):
    """
    Convenience wrapper for database UPDATE, returns affected row count.
    Keyword arguments are added to WHERE.
    """
    return init().update(table, values, where, **kwargs)


def delete(table, where=(), **kwargs):
    """
    Convenience wrapper for database DELETE, returns affected row count.
    Keyword arguments are added to WHERE.
    """
    return init().delete(table, where, **kwargs)


def execute(sql, args=None):
    """Executes the SQL and returns sqlite3.Cursor."""
    return init().execute(sql, args)


def executescript(sql):
    """Executes the SQL as script of any number of statements."""
    return init().executescript(sql)


def close():
    """
    Closes the default database connection, if any.
    """
    init().close()


def transaction(commit=True, **kwargs):
    """
    Returns a transaction context manager, breakable by raising Rollback,
    manually actionable by .commit() and .rollback().

    @param   commit  whether transaction autocommits at exit
    """
    return init().transaction(commit, **kwargs)


def register_adapter(transformer, typeclasses, engine=None):
    """
    Registers function to auto-adapt given Python types to database types in query parameters.

    Registration is global per engine.

    @param   transformer  function(Python value) returning adapted value
    @param   typeclasses  one or more Python classes to adapt
    @param   engine       database engine to adapt for, defaults to first initialized
    """
    populate_engines()
    if not isinstance(typeclasses, (list, set, tuple)): typeclasses = [typeclasses]
    engine = engine.lower() if engine else next(Database.ENGINES)
    Database.ENGINES[engine].register_adapter(transformer, typeclasses)
    #for t in typeclasses: sqlite3.register_adapter(t, transformer)


def register_converter(transformer, typenames, engine=None):
    """
    Registers function to auto-convert given SQL types to Python in query results.

    Registration is global per engine.

    @param   transformer  function(raw database value) returning Python value
    @param   typenames    one or more database column types to adapt
    @param   engine       database engine to convert for, defaults to first initialized
    """
    populate_engines()
    if isinstance(typenames, str): typenames = [typenames]
    engine = engine.lower() if engine else next(Database.ENGINES)
    Database.ENGINES[engine].register_converter(transformer, typenames)



class Queryable(object):
    """Abstract base for Database and Transaction."""

    def fetchall(self, table, cols="*", where=(), group=(), order=(), limit=(),
                 **kwargs):
        """
        Convenience wrapper for database SELECT and fetch all.
        Keyword arguments are added to WHERE.
        """
        cursor = self.select(table, cols, where, group, order, limit, **kwargs)
        return cursor.fetchall()


    def fetchone(self, table, cols="*", where=(), group=(), order=(), limit=(),
                 **kwargs):
        """
        Convenience wrapper for database SELECT and fetch one.
        Keyword arguments are added to WHERE.
        """
        limit = limit or 1
        cursor = self.select(table, cols, where, group, order, limit, **kwargs)
        return cursor.fetchone()


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        raise NotImplementedError()


    def select(self, table, cols="*", where=(), group=(), order=(), limit=(),
               **kwargs):
        """
        Convenience wrapper for database SELECT, returns sqlite3.Cursor.
        Keyword arguments are added to WHERE.
        """
        where = list(where.items() if isinstance(where, dict) else where)
        where += kwargs.items()
        sql, args = self.makeSQL("SELECT", table, cols, where, group, order, limit)
        return self.execute(sql, args)


    def update(self, table, values, where=(), **kwargs):
        """
        Convenience wrapper for database UPDATE, returns affected row count.
        Keyword arguments are added to WHERE.
        """
        where = list(where.items() if isinstance(where, dict) else where)
        where += kwargs.items()
        sql, args = self.makeSQL("UPDATE", table, values=values, where=where)
        return self.execute(sql, args).rowcount


    def delete(self, table, where=(), **kwargs):
        """
        Convenience wrapper for database DELETE, returns affected row count.
        Keyword arguments are added to WHERE.
        """
        where = list(where.items() if isinstance(where, dict) else where)
        where += kwargs.items()
        sql, args = self.makeSQL("DELETE", table, where=where)
        return self.execute(sql, args).rowcount


    def execute(self, sql, args=None):
        """Executes the SQL and returns sqlite3.Cursor."""
        raise NotImplementedError()


    def executescript(self, sql):
        """Executes the SQL as script of any number of statements."""
        raise NotImplementedError()


    def makeSQL(self, action, table, cols="*", where=(), group=(), order=(),
                limit=(), values=()):
        """Returns (SQL statement string, parameter dict)."""
        raise NotImplementedError()


    def quote(self, val, force=False):
        """
        Returns identifier in quotes and proper-escaped for queries,
        if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

        @param   force  whether to quote value even if not required
        """
        raise NotImplementedError()



class Database(Queryable):
    """Database instance. Usable as an auto-closing context manager."""

    ## Database engine modules, as {"sqlite": sqlite submodule, ..}
    ENGINES = None

    ## Created instances as {(engine name, Database.identity): Database}
    INSTANCES = collections.OrderedDict()


    @classmethod
    def factory(cls, opts, engine=None, **kwargs):
        """
        Returns a new or cached Database, or first created if opts is None.

        @param   opts    database connection options, engine-specific;
                         SQLite takes a file path or path-like object or `":memory:"`,
                         Postgres takes a Postgres URI scheme
                         like `"postgresql://user@localhost/mydb"`
                         or a Postgres keyword=value format
                         like `"host=localhost username=user dbname=mydb"`
                         or a dictionary of connection options like `dict(host="localhost", ..)`
        @param   engine  database engine if not auto-detecting from connection options,
                         "sqlite" for SQLite3 and "postgres" for PostgreSQL (case-insensitive)
        @param   kwargs  additional arguments given to engine constructor,
                         e.g. `detect_types=sqlite3.PARSE_COLNAMES` for SQLite,
                         or `minconn=1, maxconn=4` for Postgres connection pool
        """
        populate_engines()
        key, engine = None, engine.lower() if engine else None
        if opts is None and engine is None:  # Return first database, or raise
            key = next(iter(cls.INSTANCES))
        elif opts is None:  # Return first database from engine, or raise
            key = next((n, o) for n, o in cls.INSTANCES if n == engine)
        elif engine is None:  # Auto-detect engine from options, or raise
            engine = next(n for n, m in cls.ENGINES.items() if m.autodetect(opts))

        key = key or (engine, cls.ENGINES[engine].Database.make_identity(opts, **kwargs))
        if key not in cls.INSTANCES:
            db = cls.ENGINES[engine].Database(opts, **kwargs)
            cls.INSTANCES[key] = db
        cls.INSTANCES[key].open()
        return cls.INSTANCES[key]


    @classmethod
    def make_identity(cls, opts, **kwargs):
        """Returns a tuple of (connection options as string, engine arguments as string)."""
        raise NotImplementedError()


    @property
    def identity(self):
        """Tuple of (connection options as string, engine arguments as string)."""
        raise NotImplementedError()


    def __enter__(self):
        """Context manager entry, opens database if not already open, returns Database object."""
        self.open()
        return self


    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, closes database."""
        self.close()


    def transaction(self, commit=True):
        """
        Returns a transaction context manager, breakable by raising Rollback,
        manually actionable by .commit() and .rollback().

        @param   commit  whether transaction autocommits at exit
        """
        engine = next(n for (n, _), d in self.INSTANCES.items() if d is self)
        return self.ENGINES[engine].Transaction(self, commit)


    def open(self):
        """Opens database connection if not already open."""
        raise NotImplementedError()


    def close(self):
        """Closes the database, if open."""
        raise NotImplementedError()


class Transaction(Queryable):
    """Transaction context manager, breakable by raising Rollback."""

    def __init__(self, db, commit=True, exclusive=False, **kwargs):
        """
        Note that in SQLite, a single connection has one shared transaction state,
        so it is highly recommended to use exclusive Transaction instances for any action queries,
        as otherwise concurrent transactions can interfere with one another.
        The `exclusive` parameter defaults to `True` when using SQLite.

        @param   commit     if true, transaction auto-commits at the end
        @param   exclusive  whether entering a with-block is exclusive over other
                            Transaction instances entering an exclusive with-block
                            on this Database instance
        @param   kwargs     engine-specific arguments, like `schema="other", lazy=True` for Postgres
        """
        super(Transaction, self).__init__()
        self._db, self._autocommit = db, commit

    def __enter__(self):
        """Context manager entry, returns Transaction object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, propagates raised errors except Rollback."""
        return exc_type in (None, Rollback)

    def close(self, commit=None): raise NotImplementedError()
    def commit(self):             raise NotImplementedError()
    def rollback(self):           raise NotImplementedError()

    @property
    def database(self):
        """Returns transaction Database instance."""
        raise NotImplementedError()


class Rollback(Exception):
    """
    Raising in transaction context manager will roll back the transaction
    and exit the context manager cleanly, without rising further.
    """
    pass



class StaticTzInfo(datetime.tzinfo):
    """datetime.tzinfo class representing a constant offset from UTC."""
    ZERO = datetime.timedelta(0)

    def __init__(self, name, delta):
        """Constructs a new static zone info, with specified name and time delta."""
        self._name    = name
        self._offset = delta

    def utcoffset(self, dt): return self._offset
    def dst(self, dt):       return self.ZERO
    def tzname(self, dt):    return self._name
    def __ne__(self, other): return not self.__eq__(other)
    def __repr__(self):      return "%s(%s)" % (self.__class__.__name__, self._name)
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._offset == other._offset
## UTC timezone singleton
UTC = StaticTzInfo("UTC", StaticTzInfo.ZERO)



def json_loads(s):
    """
    Returns deserialized JSON, with datetime/date strings converted to objects.

    Returns original input if loading as JSON failed.
    """
    def convert_recursive(data):
        """Converts ISO datetime strings to objects in nested dicts or lists."""
        result = []
        pairs = enumerate(data) if isinstance(data, list) \
                else data.items() if isinstance(data, dict) else []
        rgx = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(([+-]\d{2}:?\d{2})|Z)?$"
        for k, v in pairs:
            if isinstance(v, (dict, list)): v = convert_recursive(v)
            elif isinstance(v, six.string_types) and len(v) > 18 and re.match(rgx, v):
                v = parse_datetime(v)
            result.append((k, v))
        return [x for _, x in result] if isinstance(data, list) \
               else dict(result) if isinstance(data, dict) else data
    try:
        return None if s is None else json.loads(s, object_hook=convert_recursive)
    except Exception:
        fails = getattr(json_loads, "__fails", set())
        if hash(s) not in fails: # Avoid spamming logs
            logger.warning("Failed to parse JSON from %r.", s, exc_info=True)
            setattr(json_loads, "__fails", fails | set([hash(s)]))
        return s


def json_dumps(data, indent=2, sort_keys=True):
    """
    Returns JSON string, with datetime types converted to ISO-8601 strings
    (in UTC if no timezone set), sets converted to lists,
    and Decimal objects converted to float or int. Returns None if data is None.
    """
    if data is None: return None
    def encoder(x):
        if isinstance(x,    set): return list(x)
        if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
            if x.tzinfo is None: x = x.replace(tzinfo=UTC)
            return x.isoformat()
        if isinstance(x, decimal.Decimal):
            return float(x) if x.as_tuple().exponent else int(x)
        return None
    return json.dumps(data, default=encoder, indent=indent, sort_keys=sort_keys)


def parse_datetime(s):
    """
    Tries to parse string as ISO8601 datetime, returns input on error.
    Supports "YYYY-MM-DD[ T]HH:MM:SS(.micros)?(Z|[+-]HH(:MM)?)?".
    All returned datetimes are timezone-aware, falling back to UTC.
    """
    if len(s) < 18: return s
    rgx = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(([+-]\d{2}(:?\d{2})?)|Z)?$"
    result, match = s, re.match(rgx, s)
    if match:
        millis, _, offset, _ = match.groups()
        minimal = re.sub(r"\D", "", s[:match.span(2)[0]] if offset else s)
        fmt = "%Y%m%d%H%M%S" + ("%f" if millis else "")
        try:
            result = datetime.datetime.strptime(minimal, fmt)
            if offset: # Support timezones like 'Z' or '+03:00'
                hh, mm = map(int, [offset[1:3], offset[4:]])
                delta = datetime.timedelta(hours=hh, minutes=mm)
                if offset.startswith("-"): delta = -delta
                result = result.replace(tzinfo=StaticTzInfo(offset, delta))
        except ValueError: pass
    if isinstance(result, datetime.datetime) and result.tzinfo is None:
        result = result.replace(tzinfo=UTC) # Force UTC timezone on unaware values
    return result



def load_modules():
    """Returns db engines loaded from file directory, as {name: module}."""
    result = {}
    for n in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "engines", "*"))):
        name = os.path.splitext(os.path.basename(n))[0]
        if name.startswith("__") or os.path.isfile(n) and not re.match(".*pyc?$", n) \
        or os.path.isdir(n) and not any(glob.glob(os.path.join(n, x)) for x in ("*.py", "*.pyc")):
            continue  # for n

        modulename = "%s.%s.%s" % (__package__, "engines", name)
        module = importlib.import_module(modulename)
        result[name] = module
    return result


def populate_engines():
    """Populates Database engines, if not already populated."""
    if Database.ENGINES is None:
        Database.ENGINES = load_modules()
