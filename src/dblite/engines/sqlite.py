# -*- coding: utf-8 -*-
"""
Simple convenience wrapper for SQLite.

    db.init(":memory:")
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

    with db.transaction() as t:
        db.insert("test", val="will be rolled back")
        db.update("test", {"val": "will be rolled back"}, id=0)
        raise db.Rollback     # Rolls back uncommitted actions and exits
        db.insert("test", val="this will never be reached")

    with db.transaction(commit=False) as t:
        db.insert("test", val="will be committed")
        t.commit()            # Commits uncommitted actions
        db.insert("test", val="will be rolled back")
        t.rollback()          # Rolls back uncommitted actions
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
        for k, v in zip(("limit", "offset"), limit or ()):
            if v is None: continue # for k, v
            sql += " %s :%s" % (k.upper(), k)
            args[k] = v

        return sql, args


    def quote(self, val, force=False):
        """
        Returns identifier in quotes and proper-escaped for queries,
        if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

        @param   force  whether to quote value even if not required
        """
        return quote(val, force)



class Database(api.Database, Queryable):
    """Convenience wrapper around sqlite3.Connection."""

    ## Mutexes for exclusive transactions, as {Database instance: lock}
    MUTEX = collections.defaultdict(threading.RLock)


    def __init__(self, path=":memory:", **kwargs):
        """
        Creates a new SQLite connection.

        @param   kwargs  suitable arguments are passed to sqlite3.connect()
        """
        super(Database, self).__init__()
        self.connection = None
        self.path       = path
        self._kwargs    = kwargs
        self._identity  = (self.path, (str(kwargs) if kwargs else ""))


    @property
    def identity(self): return self._identity


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        values = list(values.items() if isinstance(values, dict) else values)
        values += kwargs.items()
        sql, args = self.makeSQL("INSERT", table, values=values)
        return self.execute(sql, args).lastrowid


    def execute(self, sql, args=None):
        """Executes the SQL and returns sqlite3.Cursor."""
        return self.connection.execute(sql, args or {})


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


    def close(self):
        """Closes the database connection, if open."""
        try: self.connection.close()
        except Exception: pass
        self.connection = None


    @classmethod
    def make_identity(cls, opts, **kwargs):
        """Returns a tuple of (connection options as string, engine arguments as string)."""
        return (opts, str(kwargs) if kwargs else "")



class Transaction(api.Transaction, Queryable):
    """Transaction context manager, breakable by raising Rollback."""

    def __init__(self, db, commit=True, exclusive=True, **__):
        """
        Note that in SQLite, a single connection has one shared transaction state,
        so it is highly recommended to use exclusive Transaction instances for any action queries,
        as otherwise concurrent transactions can interfere with one another.

        @param   commit     if true, transaction auto-commits at the end
        @param   exclusive  whether entering a with-block is exclusive over other
                            Transaction instances entering an exclusive with-block
                            on this Database instance
        """
        super(Transaction, self).__init__(db, commit)
        self._isolevel0 = None
        self._exclusive = exclusive

    def __enter__(self):
        if self._exclusive: Database.MUTEX[self._db].acquire()
        self._isolevel0 = self._db.connection.isolation_level
        self._db.connection.isolation_level = "DEFERRED"
        return self

    def __exit__(self, exc_type, exc_val, exc_trace):
        try:
            if self._autocommit and exc_type is None: self._db.connection.commit()
            else: self._db.connection.rollback()
            self._db.connection.isolation_level = self._isolevel0
            return exc_type in (None, api.Rollback) # Do not propagate raised Rollback
        finally:
            if self._exclusive: Database.MUTEX[self._db].release()

    def close(self, commit=None):
        """
        Closes the transaction, performing commit or rollback as configured.

        @param   commit  True for final commit, False for rollback
        """
        if commit is False: self.rollback()
        elif commit:        self.commit()

    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        values = list(values.items() if isinstance(values, dict) else values)
        values += kwargs.items()
        sql, args = self.makeSQL("INSERT", table, values=values)
        return self.execute(sql, args).lastrowid

    def execute(self, sql, args=None):
        """Executes the SQL and returns sqlite3.Cursor."""
        return self._db.connection.execute(sql, args or {})

    def executescript(self, sql):
        """Executes the SQL as script of any number of statements."""
        self._db.connection.executescript(sql)

    def commit(self):   self._db.connection.commit()
    def rollback(self): self._db.connection.rollback()
        


def autodetect(opts):
    """
    Returns true if inputs are recognizable as SQLite connection options.

    @param   opts    expected as a path string or path-like object
    """
    if isinstance(opts, string_types):  # E.g. not "postgresql://"
        return opts.startswith("file:") or not re.match(r"^\w+\:\/\/", opts)
    elif sys.version_info >= (3, 4):
        import pathlib
        return isinstance(opts, pathlib.Path)
    return False


def quote(val, force=False):
    """
    Returns identifier in quotes and proper-escaped for queries,
    if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

    @param   force  whether to quote value even if not required
    """
    if not isinstance(val, string_types):
        return val
    RGX_INVALID = r"(^[\W\d])|(?=\W)"
    result = val.decode() if isinstance(val, binary_type) else val
    if force or result.upper() in RESERVED_KEYWORDS or re.search(RGX_INVALID, result, re.U):
        result = u'"%s"' % result.replace('"', '""')
    return result


def register_adapter(transformer, typeclasses):
    """Registers function to auto-adapt given Python types to SQLite in query parameters."""
    for t in typeclasses: sqlite3.register_adapter(t, transformer)


def register_converter(transformer, typenames):
    """Registers function to auto-convert given SQL types to Python in query results."""
    for n in typenames: sqlite3.register_converter(n, transformer)



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
