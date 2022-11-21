# -*- coding: utf-8 -*-
"""
Simple convenience wrapper for SQLite.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     05.03.2014
@modified    21.11.2022
------------------------------------------------------------------------------
"""
import collections
import logging
import os
import re
import sqlite3
import sys
import threading

from six import binary_type, integer_types, string_types

from .. import api

logger = logging.getLogger(__name__)


## SQLite reserved keywords, needing quotes in SQL queries
RESERVED_KEYWORDS = [
    "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS", "ASC", "ATTACH",
    "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASE", "CAST", "CHECK", "COLLATE",
    "COMMIT", "CONSTRAINT", "CREATE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP",
    "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "EXPLAIN", "FOR", "FOREIGN", "FROM",
    "GENERATED", "GROUP", "HAVING", "IF", "IMMEDIATE", "IN", "INDEX", "INITIALLY", "INSERT",
    "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LIKE", "LIMIT", "MATCH",
    "NO", "NOT", "NOTHING", "NOTNULL", "NULL", "OF", "ON", "OR", "ORDER", "OVER", "PRAGMA",
    "PRECEDING", "PRIMARY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE",
    "RENAME", "REPLACE", "RESTRICT", "ROLLBACK", "SAVEPOINT", "SELECT", "SET", "TABLE",
    "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION",
    "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "WHEN", "WHERE", "WITHOUT"
]


class Queryable(api.Queryable):

    OPS = ["||", "*", "/", "%", "+", "-", "<<", ">>", "&", "|", "<", "<=", ">",
           ">=", "=", "==", "!=", "<>", "IS", "IS NOT", "IN", "NOT IN", "LIKE",
           "GLOB", "MATCH", "REGEXP", "AND", "OR"]


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        values = list(values.items() if isinstance(values, dict) else values)
        values += kwargs.items()
        sql, args = self.makeSQL("INSERT", table, values=values)
        return self.execute(sql, args).lastrowid


    def makeSQL(self, action, table, cols="*", where=(), group=(), order=(),
                limit=(), values=()):
        """Returns (SQL statement string, parameter dict)."""

        def cast(col, val):
            """Returns column value cast to correct type for use in sqlite."""
            return tuple(val) if isinstance(val, set) else val

        def parse_members(i, col, op, val):
            """Returns (col, op, val, argkey)."""
            key = "%sW%s" % (re.sub(r"\W+", "_", col), i)
            if "EXPR" == col.upper():
                # ("EXPR", ("SQL", val))
                col, op, val, key = val[0], "EXPR", val[1], "EXPRW%s" % i
            elif col.count("?") == argcount(val):
                # ("any SQL with ? placeholders", val)
                op, val, key = "EXPR", listify(val), "EXPRW%s" % i
            elif isinstance(val, (list, tuple)) and len(val) == 2 \
            and isinstance(val[0], string_types):
                tmp = val[0].strip().upper()
                if tmp in self.OPS: # ("col", ("binary op like >=", val))
                    op, val = tmp, val[1]
                elif val[0].count("?") == argcount(val[1]):
                    # ("col", ("SQL with ? placeholders", val))
                    col, val, op = "%s = %s" % (col, val[0]), listify(val[1]), "EXPR"
            return col, op, val, key
        def argcount(x): return len(x) if isinstance(x, (list, set, tuple)) else 1
        def listify(x) : return x if isinstance(x, (list, tuple)) else [x]

        action = action.upper()
        cols   =    cols if isinstance(cols,  string_types) else ", ".join(cols)
        where  = [where] if isinstance(where, string_types) else where
        group  =   group if isinstance(group, string_types) else ", ".join(map(str, listify(group)))
        order  = [order] if isinstance(order, string_types) else order
        order  = [order] if isinstance(order, (list, tuple)) \
                 and len(order) == 2 and isinstance(order[1], bool) else order
        limit  = [limit] if isinstance(limit, string_types + integer_types) else limit
        values = values if not isinstance(values, dict) else values.items()
        where  =  where if not isinstance(where,  dict)  else where.items()
        sql = "SELECT %s FROM %s" % (cols, table) if "SELECT" == action else ""
        sql = "DELETE FROM %s"    % (table)       if "DELETE" == action else sql
        sql = "INSERT INTO %s"    % (table)       if "INSERT" == action else sql
        sql = "UPDATE %s"         % (table)       if "UPDATE" == action else sql
        args = {}

        if "INSERT" == action:
            keys = ["%sI%s" % (re.sub(r"\W+", "_", k), i) for i, (k, _) in enumerate(values)]
            args.update((n, cast(k, v)) for n, (k, v) in zip(keys, values))
            cols, vals = ", ".join(k for k, _ in values), ", ".join(":%s" % n for n in keys)
            sql += " (%s) VALUES (%s)" % (cols, vals)
        if "UPDATE" == action:
            sql += " SET "
            for i, (col, val) in enumerate(values):
                key = "%sU%s" % (re.sub(r"\W+", "_", col), i)
                sql += (", " if i else "") + "%s = :%s" % (col, key)
                args[key] = cast(col, val)
        if where:
            sql += " WHERE "
            for i, clause in enumerate(where):
                if isinstance(clause, string_types): # "raw SQL with no arguments"
                    clause = (clause, )

                if len(clause) == 1: # ("raw SQL with no arguments", )
                    col, op, val, key = clause[0], "EXPR", [], None
                elif len(clause) == 2: # ("col", val) or ("col", ("op" or "expr with ?", val))
                    col, op, val, key = parse_members(i, clause[0], "=", clause[1])
                else: # ("col", "op" or "expr with ?", val)
                    col, op, val, key = parse_members(i, *clause)

                if op in ("IN", "NOT IN"):
                    keys = ["%s_%s" % (key, j) for j in range(len(val))]
                    args.update({k: cast(col, v) for k, v in zip(keys, val)})
                    sql += (" AND " if i else "") + "%s %s (%s)" % (
                            col, op, ", ".join(":" + x for x in keys))
                elif "EXPR" == op:
                    for j in range(col.count("?")):
                        col = col.replace("?", ":%s_%s" % (key, j), 1)
                        args["%s_%s" % (key, j)] = cast(None, val[j])
                    sql += (" AND " if i else "") + "(%s)" % col
                elif val is None:
                    op = {"=": "IS", "!=": "IS NOT", "<>": "IS NOT"}.get(op, op)
                    sql += (" AND " if i else "") + "%s %s NULL" % (col, op)
                else:
                    args[key] = cast(col, val)
                    sql += (" AND " if i else "") + "%s %s :%s" % (col, op, key)
        if group:
            sql += " GROUP BY " + group
        if order:
            sql += " ORDER BY "
            for i, col in enumerate(order):
                name = col if isinstance(col, string_types) else col[0]
                sort = col[1] if name != col and len(col) > 1 else ""
                if not isinstance(sort, string_types): sort = "DESC" if sort else ""
                sql += (", " if i else "") + name + (" " if sort else "") + sort
        if limit:
            limit = [None if isinstance(v, integer_types) and v < 0 else v for v in limit]
            for i, (k, v) in enumerate(zip(("limit", "offset"), limit)):
                if v is None:
                    if i or len(limit) < 2 or not limit[1]: continue  # for i, (k, v)
                    v = -1  # LIMIT is required if OFFSET
                sql += " %s :%s" % (k.upper(), k)
                args[k] = v

        return sql, args


    @classmethod
    def quote(cls, value, force=False):
        """
        Returns identifier in quotes and proper-escaped for queries,
        if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

        @param   force  whether to quote value even if not required
        """
        return quote(value, force)



class Database(api.Database, Queryable):
    """
    Convenience wrapper around sqlite3.Connection.

    Queries directly on the Database object use autocommit mode.
    """

    ## Mutexes for exclusive actions, as {Database instance: lock}
    MUTEX = collections.defaultdict(threading.RLock)


    def __init__(self, opts=":memory:", **kwargs):
        """
        Creates a new Database instance for SQLite.

        @param   kwargs  supported arguments are passed to sqlite3.connect() in open(),
                         like `detect_types=sqlite3.PARSE_COLNAMES`
        """
        super(Database, self).__init__()
        self.connection = None
        self.path       = opts
        self._kwargs    = kwargs
        self._txs       = []  # [Transaction, ]


    def __enter__(self):
        """Context manager entry, opens database if not already open, returns Database object."""
        self.open()
        return self


    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, closes database and any pending transactions if open."""
        txs, self._txs[:] = self._txs[:], []
        for tx in txs: tx.close(commit=None if exc_type is None else False)
        self.close()
        return exc_type is None


    def execute(self, sql, args=()):
        """Executes the SQL statement and returns sqlite3.Cursor."""
        return self.connection.execute(sql, args)


    def executescript(self, sql):
        """Executes the SQL as script of any number of statements."""
        self.connection.executescript(sql)


    def open(self):
        """Opens the database connection, if not already open."""
        if self.connection: return
        KWS = ("timeout", "detect_types", "isolation_level", "check_same_thread",
               "factory", "cached_statements", "uri")
        args = dict(detect_types=sqlite3.PARSE_DECLTYPES,
                    isolation_level=None, check_same_thread=False)
        args.update({k: v for k, v in self._kwargs.items() if k in KWS})
        if ":memory:" != self.path and not os.path.exists(self.path):
            try: os.makedirs(os.path.dirname(self.path))
            except Exception: pass
        conn = sqlite3.connect(self.path, **args)
        conn.row_factory = lambda cursor, row: dict(sqlite3.Row(cursor, row))
        self.connection = conn


    def close(self, commit=None):
        """
        Closes the database and any pending transactions, if open.

        @param   commit  `True` for explicit commit on open transactions,
                         `False` for explicit rollback on open transactions,
                         `None` defaults to `commit` flag from transaction creations
        """
        txs, self._txs[:] = self._txs[:], []
        for tx in txs: tx.close(commit)
        if self.connection:
            self.connection.close()
            self.connection = None

    @property
    def closed(self):
        """Whether database is currently not open."""
        return not self.connection


    def transaction(self, commit=True, exclusive=True, **kwargs):
        """
        Returns a transaction context manager.

        Context is breakable by raising Rollback.

        @param   commit     whether transaction commits automatically at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive
                            over other Transaction instances on this Database
        @param   kwargs     engine-specific arguments, like `detect_types=sqlite3.PARSE_COLNAMES`
        """
        tx = Transaction(self, commit, exclusive, **kwargs)
        self._txs.append(tx)
        return tx


    def _notify(self, tx):
        """Notifies database of transaction closing."""
        if tx in self._txs: self._txs.remove(tx)



class Transaction(api.Transaction, Queryable):
    """
    Transaction context manager, breakable by raising Rollback.

    Note that in SQLite, a single connection has one shared transaction state,
    so it is highly recommended to use exclusive Transaction instances for any action queries,
    as concurrent transactions can interfere with one another otherwise.
    """

    def __init__(self, db, commit=True, exclusive=True, **__):
        """
        Creates a new transaction.

        @param   commit     whether transaction commits automatically at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive over other
                            Transaction instances on this Database
        """
        self._db         = db
        self._exitcommit = commit
        self._enterstack = 0  # Number of levels the transaction context is nested at
        self._isolevel0  = None
        self._exclusive  = True if exclusive is None else exclusive
        self._closed     = False
        self._cursor     = None

    def __enter__(self):
        """Context manager entry, opens cursor, returns Transaction object."""
        if self._closed: raise RuntimeError("Transaction already closed")

        if self._exclusive: Database.MUTEX[self._db].acquire()
        if not self._cursor:
            self._isolevel0 = self._db.connection.isolation_level
            self._db.connection.isolation_level = "DEFERRED"
            try: self._cursor = self._db.execute("SAVEPOINT tx%s" % id(self))
            except Exception:
                if self._exclusive: Database.MUTEX[self._db].release()
                raise
        self._enterstack += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, closes cursor, commits or rolls back as specified on creation."""
        depth = self._enterstack = self._enterstack - 1
        try:
            if self._cursor:
                self.commit() if self._exitcommit and exc_type is None else self.rollback()
            return exc_type in (None, api.Rollback) # Do not propagate raised Rollback
        finally:
            if depth < 1:
                self._cursor = None
                self._closed = True
                self._db.connection.isolation_level = self._isolevel0
                self._db._notify(self)
            if self._exclusive: Database.MUTEX[self._db].release()

    def close(self, commit=None):
        """
        Closes the transaction, performing commit or rollback as specified.

        @param   commit  `True` for final commit, `False` for rollback,
                         `None` for auto-commit, if any
        """
        if self._closed:
            self._db._notify(self)
            return
        if commit is False: self.rollback()
        elif commit or self._exitcommit: self.commit()
        self._closed = True
        self._db.connection.isolation_level = self._isolevel0
        self._db._notify(self)

    def execute(self, sql, args=()):
        """Executes the SQL statement and returns sqlite3.Cursor."""
        if self._closed: raise RuntimeError("Transaction already closed")
        if not self._cursor:
            self._isolevel0 = self._db.connection.isolation_level
            self._db.connection.isolation_level = "DEFERRED"
            self._cursor = self._db.execute("SAVEPOINT tx%s" % id(self))
        return self._cursor.execute(sql, args)

    def executescript(self, sql):
        """
        Executes the SQL as script of any number of statements, outside of transaction.

        Any pending transaction will be committed first.
        """
        if self._closed: raise RuntimeError("Transaction already closed")
        with Database.MUTEX[self._db]:
            self._reset(commit=True)
            self._db.executescript(sql)

    def commit(self):
        """Commits pending actions, if any."""
        if not self._cursor: return
        with Database.MUTEX[self._db]:
            self._reset(commit=True)

    def rollback(self):
        """Rolls back pending actions, if any."""
        if not self._cursor: return
        with Database.MUTEX[self._db]:
            self._reset(commit=False)

    @property
    def closed(self):
        """Whether transaction is currently not open."""
        return self._closed

    @property
    def database(self):
        """Returns transaction Database instance."""
        return self._db

    def _reset(self, commit=False):
        """Commits or rolls back ongoing transaction, if any, closes cursor, if any."""
        if getattr(self._db.connection, "in_transaction", True):  # Py 3.2+
            self._db.connection.commit() if commit else self._db.connection.rollback()
        if self._cursor:
            self._cursor.close()
            self._cursor = None



def autodetect(opts):
    """
    Returns true if input is recognizable as SQLite connection options.

    @param   opts    expected as a path string or path-like object
    """
    if isinstance(opts, string_types):  # E.g. not "postgresql://"
        return opts.startswith("file:") or not re.match(r"^\w+\:\/\/", opts)
    elif sys.version_info >= (3, 4):
        import pathlib
        return isinstance(opts, pathlib.Path)
    return False


def quote(value, force=False):
    """
    Returns identifier in quotes and proper-escaped for queries,
    if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

    @param   force  whether to quote value even if not required
    """
    if not isinstance(value, string_types):
        return value
    RGX_INVALID = r"(^[\W\d])|(?=\W)"
    result = value.decode() if isinstance(value, binary_type) else value
    if force or result.upper() in RESERVED_KEYWORDS or re.search(RGX_INVALID, result, re.U):
        result = u'"%s"' % result.replace('"', '""')
    return result


def register_adapter(transformer, typeclasses):
    """Registers function to auto-adapt given Python types to SQLite types in query parameters."""
    for t in typeclasses: sqlite3.register_adapter(t, transformer)


def register_converter(transformer, typenames):
    """Registers function to auto-convert given SQLite types to Python types in query results."""
    for n in typenames: sqlite3.register_converter(n, transformer)


__all__ = [
    "RESERVED_KEYWORDS", "Database", "Transaction",
    "autodetect", "quote", "register_adapter", "register_converter",
]


if "__main__" == __name__:
    def test():
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        import dblite as db
        db.init(":memory:")
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")

        print("Inserted ID %s." % db.insert("test", val=None))
        for i in range(5): print("Inserted ID %s." % db.insert("test", {"val": i}))
        print("Fetch ID 1: %s." % db.fetchone("test", id=1))
        print("Fetch all up to 3, order by val: %s." % db.fetchall("test", order="val", limit=3))
        print("Updated %s row where val is NULL." % db.update("test", {"val": "new"}, val=None))
        print("Select where val IN [0, 1, 2]: %s." % db.fetchall("test", val=("IN", range(3))))
        print("Delete %s row where val=0." % db.delete("test", val=0))
        with db.transaction():
            print("Delete %s row where val=1, and roll back." % db.delete("test", val=1))
            raise db.Rollback
        print("Fetch all, order by val: %s." % db.fetchall("test", order="val"))
        print("Select with expression: %s." % db.fetchall("test", EXPR=("id IN (SELECT id FROM test WHERE id in (?, ?, ?))", [1, 3, 5])))
        db.execute("DROP TABLE test")
        db.close()
    test()
