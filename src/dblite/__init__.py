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
@modified    16.11.2022
"""
import collections
import base64
import datetime
import decimal
import glob
import imghdr
import importlib
import json
import logging
import os
import re

import pytz

logger = logging.getLogger(__name__)


def init(engine=None, opts=None, **kwargs):
    """
    Returns a Database object, creating one if not already open with these opts.
    If engine and opts is None, returns the default database - the very first initialized.
    Module level functions use the default database.
    """
    return Database.factory(engine, opts, **kwargs)


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


def close(cascade=False):
    """
    Closes the default database connection, if any.
    If cascade, closes all database Transactions as well.
    """
    init().close(cascade)


def transaction(commit=True, **kwargs):
    """
    Returns a transaction context manager, breakable by raising Rollback,
    manually actionable by .commit() and .rollback().

    @param   commit  whether transaction autocommits at exit
    """
    return init().transaction(commit, **kwargs)



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



class Database(Queryable):
    """Database instance."""

    CACHE   = collections.OrderedDict()      # {(engine name, opts json): Database}
    TXS     = collections.defaultdict(list)  # {Database: [Transaction, ], }
    ENGINES = None                           # {"sqlite": sqlite submodule, }


    @classmethod
    def factory(cls, engine, opts, **kwargs):
        """
        Returns a new or cached Database, or first created if opts is None.
        """
        key = next(iter(cls.CACHE)) if opts is None else (engine, json_dumps(opts))

        if key in cls.CACHE: cls.CACHE[key].open()
        else:
            if cls.ENGINES is None:
                cls.ENGINES = load_modules()

            db = cls.ENGINES[engine].Database(opts, **kwargs)
            db.engine = cls.ENGINES[engine]
            cls.CACHE[key] = db
        return cls.CACHE[key]


    def transaction(self, commit=True):
        """
        Returns a transaction context manager, breakable by raising Rollback,
        manually actionable by .commit() and .rollback().

        @param   commit  whether transaction autocommits at exit
        """
        tx = self.engine.Transaction(self, commit)
        self.TXS[self].append(tx)
        return tx


    def open(self):
        """Opens database connection if not already open."""
        raise NotImplementedError()


    def close(self, cascade=False):
        """Closes connection. If cascade, closes all transactions also."""
        if not cascade or self not in self.TXS: return
        for tx in self.TXS[self]: tx.close()
        self.TXS.pop(self, None)



class Transaction(Queryable):
    """Transaction context manager, breakable by raising Rollback."""

    def __init__(self, db, commit=True, **kwargs):
        super(Transaction, self).__init__()
        self._db, self._autocommit = db, commit

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_trace):
        return exc_type in (None, Rollback) # Do not propagate raised Rollback

    def close(self, commit=None):
        """Removes transaction from Database cache."""
        if self in Database.TXS.get(self._db, []):
            Database.TXS[self._db].remove(self)

    def commit(self):             raise NotImplementedError()
    def rollback(self):           raise NotImplementedError()
        

class Rollback(StandardError):
    """
    Raising in transaction context manager will roll back the transaction
    and exit the context manager cleanly, without rising further.
    """


def json_loads(s):
    '''Returns deserialized JSON, with datetime/date strings converted to objects.'''
    def convert_recursive(data):
        '''Converts ISO datetime strings to objects in nested dicts or lists.'''
        result = []
        pairs = enumerate(data) if isinstance(data, list) \
                else data.items() if isinstance(data, dict) else []
        rgx = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(([+-]\d{2}:?\d{2})|Z)?$"
        for k, v in pairs:
            if isinstance(v, (dict, list)): v = convert_recursive(v)
            elif isinstance(v, basestring) and len(v) > 18 and re.match(rgx, v):
                v = parse_datetime(v)
            result.append((k, v))
        return [x for _, x in result] if isinstance(data, list) \
               else dict(result) if isinstance(data, dict) else data
    try:
        return None if s is None else json.loads(s, object_hook=convert_recursive)
    except Exception:
        fails = getattr(json_loads, "__fails", set())
        if hash(s) not in fails: # Avoid spamming logs
            logger.warn("Failed to parse JSON from %r.", s, exc_info=True)
            setattr(json_loads, "__fails", fails | set([hash(s)]))
        return s


def json_dumps(data, indent=2, sort_keys=True):
    '''
    Returns JSON string, with datetime types converted to ISO8601 strings
    (in UTC if no timezone set), sets converted to lists,
    buffers converted to 'data:MEDIATYPE/SUBTYPE,base64,B64DATA',
    and Decimal objects converted to float or int. Returns None if data is None.
    '''
    if data is None: return None
    def encoder(x):
        if isinstance(x, buffer): return encode_b64_mime(x)
        if isinstance(x,    set): return list(x)
        if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
            if x.tzinfo is None: x = pytz.utc.localize(x)
            return x.isoformat()
        if isinstance(x, decimal.Decimal):
            return float(x) if x.as_tuple().exponent else int(x)
    return json.dumps(data, default=encoder, indent=indent, sort_keys=sort_keys)


def encode_b64_mime(buf):
    """Returns the buffer/string data like 'data:image/png,base64,iVBOR..'."""
    subtype = imghdr.what(file=None, h=buf)
    media = "image" if subtype else "application"
    subtype = subtype or "octet-stream"
    result = "data:%s/%s;base64,%s" % (media, subtype, base64.b64encode(buf))
    return result


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
                z = pytz.tzinfo.StaticTzInfo()
                z._utcoffset, z._tzname, z.zone = delta, offset, offset
                result = z.localize(result)
        except ValueError: pass
    if isinstance(result, datetime.datetime) and result.tzinfo is None:
        result = pytz.utc.localize(result) # Force UTC timezone on unaware values
    return result



def load_modules():
    """Returns db engines loaded from file directory, as {name: module}."""
    result = {}
    for f in glob.glob(os.path.join(os.path.dirname(__file__), "*")):
        if f.startswith("__") or os.path.isfile(f) and not re.match(".*pyc?$", f) \
        or os.path.isdir(f) and not any(glob.glob(os.path.join(f, x)) for x in ("*.py", "*.pyc")):
            continue # for f

        name = os.path.splitext(os.path.split(f)[-1])[0]
        modulename = "%s.%s" % (__package__, name)
        module = importlib.import_module(modulename)
        result[name] = module
    return result
