# -*- coding: utf-8 -*-
"""
Simple convenience wrapper for database connections and queries.

Provides shorthand functions for operating on a single database connection,
and similar interface to multiple databases via returned Database and Transaction objects.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     05.03.2014
@modified    2ˇ8.11.2022
------------------------------------------------------------------------------
"""
import collections
import inspect
import logging

from . import util

logger = logging.getLogger(__name__)


def init(opts=None, engine=None, **kwargs):
    """
    Returns a Database object.

    If opts is `None`, returns the default database - the very first created.
    Module level functions use the default database.

    @param   opts    database connection options, engine-specific;
                     SQLite takes a file path or path-like object or `":memory:"`,
                     Postgres takes a Postgres URI scheme like `"postgresql://user@localhost/mydb"`
                     or a Postgres keyword=value format string like
                     `"host=localhost username=user dbname=mydb"`
                     or a dictionary of connection options like `dict(host="localhost", dbname=..)`
    @param   engine  database engine if not auto-detecting from connection options,
                     `"sqlite"` for SQLite3 and `"postgres"` for PostgreSQL (case-insensitive)
    @param   kwargs  additional arguments given to engine constructor,
                     e.g. `detect_types=sqlite3.PARSE_COLNAMES` for SQLite,
                     or `minconn=1, maxconn=4` for Postgres connection pool
    """
    return Engines.factory(opts, engine, **kwargs)



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
    Convenience wrapper for database SELECT, returns database cursor.

    @param   table   table name or expression to select from
    @param   where   columns or expressions to select by, as dict or a sequence
                     of key-value tuples; value can specify operator
                     e.g. `{"mycolumn": ("IN", [1, 2, 3])}`
    @param   cols    columns to select if not all, as string or a sequence of stringables
    @param   group   columns to GROUP BY, as string or a sequence of stringables
    @param   order   columns to ORDER BY, as string, a sequence of stringables,
                     or a combination of column names and direction flags
    @param   limit   LIMIT .. OFFSET.. values, as integer or a sequence of integers;
                     None or -1 disables LIMIT or OFFSET
    @param   kwargs  additional arguments added to WHERE clause
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


def execute(sql, args=()):
    """
    Executes the SQL statement and returns database cursor.

    @param   sql   SQL statement to execute, with engine-specific parameter bindings, if any
    @param   args  query parameters, as tuple or dictionary
    """
    return init().execute(sql, args)


def executescript(sql):
    """
    Executes the SQL as script of any number of statements.

    Note that in SQLite, the statements are executed outside of transaction,
    and any pending transaction will be committed first.

    @param   sql   script with one or more SQL statements
    """
    return init().executescript(sql)


def close(commit=None):
    """
    Closes the default database connection, if any.

    @param   commit  `True` for explicit commit on open transactions,
                     `False` for explicit rollback on open transactions,
                     `None` defaults to `commit` flag from transaction creations
    """
    init().close(commit)


def transaction(commit=True, exclusive=None, **kwargs):
    """
    Returns a transaction context manager.

    Context is breakable by raising Rollback.

    Note that parameter `exclusive` defaults to `True` when using SQLite.

    @param   commit     whether transaction commits automatically at exiting with-block
    @param   exclusive  whether entering a with-block is exclusive over other
                        Transaction instances on this Database;
                        `None` stands for engine default
    @param   kwargs     engine-specific arguments, like `schema="other", lazy=True` for Postgres
    """
    return init().transaction(commit, exclusive, **kwargs)


def register_adapter(transformer, typeclasses, engine=None):
    """
    Registers function to auto-adapt given Python types to database types in query parameters.

    Registration is global per engine.

    @param   transformer  function(Python value) returning adapted value
    @param   typeclasses  one or more Python classes to adapt
    @param   engine       database engine to adapt for, defaults to first initialized
    """
    if not isinstance(typeclasses, (list, set, tuple)): typeclasses = [typeclasses]
    Engines.get(engine).register_adapter(transformer, typeclasses)


def register_converter(transformer, typenames, engine=None):
    """
    Registers function to auto-convert given database types to Python in query results.

    Registration is global per engine.

    @param   transformer  function(raw database value) returning Python value
    @param   typenames    one or more database column types to adapt
    @param   engine       database engine to convert for, defaults to first initialized
    """
    if isinstance(typenames, str): typenames = [typenames]
    Engines.get(engine).register_converter(transformer, typenames)


class Queryable(object):
    """Abstract base for Database and Transaction."""

    def fetchall(self, table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
        """
        Convenience wrapper for database SELECT and fetch all.
        Keyword arguments are added to WHERE.
        """
        return list(self.select(table, cols, where, group, order, limit, **kwargs))


    def fetchone(self, table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
        """
        Convenience wrapper for database SELECT and fetch one.
        Keyword arguments are added to WHERE.
        """
        limit = 1 if not limit and limit != 0 else limit
        return next(self.select(table, cols, where, group, order, limit, **kwargs), None)


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        raise NotImplementedError()


    def select(self, table, cols="*", where=(), group=(), order=(), limit=(), **kwargs):
        """
        Convenience wrapper for database SELECT, returns database cursor.
        Keyword arguments are added to WHERE.
        """
        sql, args = self.makeSQL("SELECT", table, cols, where, group, order, limit, kwargs=kwargs)
        cursor = self.execute(sql, args)
        return TypeCursor(cursor, table) if inspect.isclass(table) else cursor


    def update(self, table, values, where=(), **kwargs):
        """
        Convenience wrapper for database UPDATE, returns affected row count.
        Keyword arguments are added to WHERE.
        """
        sql, args = self.makeSQL("UPDATE", table, values=values, where=where, kwargs=kwargs)
        return self.execute(sql, args).rowcount


    def delete(self, table, where=(), **kwargs):
        """
        Convenience wrapper for database DELETE, returns affected row count.
        Keyword arguments are added to WHERE.
        """
        sql, args = self.makeSQL("DELETE", table, where=where, kwargs=kwargs)
        return self.execute(sql, args).rowcount


    def execute(self, sql, args=()):
        """
        Executes the SQL statement and returns database cursor.

        @param   sql   SQL statement to execute, with engine-specific parameter bindings, if any
        @param   args  query parameters, as tuple or dictionary
        """
        raise NotImplementedError()


    def executescript(self, sql):
        """
        Executes the SQL as script of any number of statements.

        Note that in SQLite, the statements are executed outside of transaction,
        and any pending transaction will be committed first.

        @param   sql   script with one or more SQL statements
        """
        raise NotImplementedError()


    def makeSQL(self, action, table, cols="*", where=(), group=(), order=(), limit=(), values=(),
                kwargs=None):
        """Returns (SQL statement string, parameter dict)."""
        raise NotImplementedError()


    @property
    def closed(self):
        """Whether currently not open."""
        raise NotImplementedError()


    @property
    def cursor(self):
        """Database engine cursor object, or `None` if closed."""
        raise NotImplementedError()


    @classmethod
    def quote(cls, value, force=False):
        """
        Returns identifier in quotes and proper-escaped for queries,
        if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

        @param   value  the value to quote, returned as-is if not string
        @param   force  whether to quote value even if not required
        """
        raise NotImplementedError()



class Database(Queryable):
    """
    Database instance. Usable as an auto-closing context manager.

    Queries directly on the Database object use autocommit mode.

    Note that the database connection is not opened immediately on construction.
    """

    def transaction(self, commit=True, exclusive=None, **kwargs):
        """
        Returns a transaction context manager.

        Context is breakable by raising Rollback.

        Note that parameter `exclusive` defaults to `True` when using SQLite.

        @param   commit     whether transaction autocommits at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive over other
                            Transaction instances on this Database;
                            `None` stands for engine default
        @param   kwargs     engine-specific arguments, like `schema="other", lazy=True` for Postgres
        """
        raise NotImplementedError()

    def __enter__(self):
        """Context manager entry, opens database if not already open, returns Database object."""
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, closes database and any pending transactions if open."""
        raise NotImplementedError()

    def __del__(self):
        """Closes the database, if open."""
        self.close()

    def open(self):
        """Opens database connection if not already open."""
        raise NotImplementedError()

    def close(self, commit=None):
        """
        Closes the database and any pending transactions, if open.

        @param   commit  `True` for explicit commit on open transactions,
                         `False` for explicit rollback on open transactions,
                         `None` defaults to `commit` flag from transaction creations
        """
        raise NotImplementedError()


class Transaction(Queryable):
    """
    Transaction context manager, breakable by raising Rollback.

    Note that in SQLite, a single connection has one shared transaction state,
    so it is highly recommended to use exclusive Transaction instances for any action queries,
    as concurrent transactions can interfere with one another otherwise.
    """

    def __init__(self, db, commit=True, exclusive=False, **kwargs):
        """
        Constructs a new transaction.

        Note that parameter `exclusive` defaults to `True` when using SQLite.

        @param   db         Database instance
        @param   commit     whether transaction commits automatically at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive
                            over other Transaction instances on this Database
        @param   kwargs     engine-specific arguments, like `schema="other", lazy=True` for Postgres
        """
        raise NotImplementedError()

    def __enter__(self):
        """Context manager entry, returns Transaction object."""
        raise NotImplementedError()

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, propagates raised errors except Rollback."""
        raise NotImplementedError()

    def __del__(self):
        """Closes the transaction, if open."""
        self.close()

    def close(self, commit=None):
        """
        Closes the transaction, performing commit or rollback as specified.
        Required if not using transaction as context manager in a with-block.

        @param   commit  `True` for explicit commit, `False` for explicit rollback,
                         `None` defaults to `commit` flag from creation
        """
        raise NotImplementedError()

    def commit(self):
        """Commits pending actions, if any."""
        raise NotImplementedError()

    def rollback(self):
        """Rolls back pending actions, if any."""
        raise NotImplementedError()

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



# ---------------------------------- detail ----------------------------------

class Engines(object):
    """Database engine broker."""

    ## Database engine modules, as {"sqlite": sqlite submodule, ..}
    MODULES = None

    ## Default Database instances as {engine name: Database}
    DATABASES = collections.OrderedDict()

    @classmethod
    def factory(cls, opts, engine=None, **kwargs):
        """
        Returns an opened Database, the first created if opts is `None`.

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
        cls.populate()
        engine = engine.lower() if engine else None
        if opts is None and engine is None:  # Return first database, or raise
            engine = next(iter(cls.DATABASES))
        elif opts is not None:  # Auto-detect engine from options, or raise
            engine = next(n for n, m in cls.MODULES.items() if m.autodetect(opts))
        db = cls.DATABASES[engine] if opts is None else cls.MODULES[engine].Database(opts, **kwargs)
        cls.DATABASES.setdefault(engine, db)
        db.open()
        return db

    @classmethod
    def get(cls, engine=None):
        """Returns engine module, by default the first created."""
        cls.populate()
        engine = engine.lower() if engine else next(iter(cls.DATABASES))
        return cls.MODULES[engine]

    @classmethod
    def populate(cls):
        """Populates Database engines, if not already populated."""
        if cls.MODULES is None: cls.MODULES = util.load_modules()


class TypeCursor(object):
    """Wrapper for database cursor, yielding rows constructed with given callable."""

    def __init__(self, cursor, callable):
        """
        @param   cursor    database engine cursor instance
        @param   callable  function(rowdict) or function(*row values) or function(**rowdict)
        """
        self.__cursor = cursor
        self.__cls    = callable
        self._logged = False
        for name, value in inspect.getmembers(cursor):
            if not hasattr(self, name):  # Monkey-patch cursor members to self
                setattr(self, name, value)

    def fetchmany(self, size=None):
        result = []
        for _ in range(self.__cursor.arraysize if size is None else size):
            row = next(self.__cursor, None)
            if row is None: break  # for
            result.append(self.__factory(row))
        return result

    def fetchone(self): return next(self, None)
    def fetchall(self): return list(self)
    def __iter__(self): return iter(self.__factory(x) for x in self.__cursor)
    def __next__(self): return self.__factory(next(self.__cursor))
    def next(self):     return self.__factory(next(self.__cursor))

    def __factory(self, row):
        """Returns row constructed with callable, or original row if all argument options failed."""
        errors = []
        try: return self.__cls(**row)          # Constructed with keyword args as row keys-values
        except Exception as e: errors.append(e)
        try: return self.__cls(*row.values())  # Constructed with positional args as row values
        except Exception as e: errors.append(e)
        try: return self.__cls(row)            # Constructed with row as single arg
        except Exception as e: errors.append(e)
        if issubclass(self.__cls, tuple) and hasattr(self.__cls, "_fields"):
            # collections.namedtuple: populate any missing fields with None
            try: return self.__cls(**dict({k: None for k in self.__cls._fields}, **row))
            except Exception as e: errors.append(e)
        if not self._logged:
            logger.warning("Failed to instantiate %s with keywords, posargs, and dictionary. "
                           "Returning dictionary.\n%s", self.__cls, "\n".join(map(repr, errors)))
            self._logged = True
        return row


__all__ = [
    "Database", "Rollback", "Transaction",
    "init", "fetchall", "fetchone", "insert", "select", "update", "delete", "execute",
    "executescript", "close", "transaction", "register_adapter", "register_converter",
]
