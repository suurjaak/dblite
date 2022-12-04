# -*- coding: utf-8 -*-
"""
Simple convenience wrapper for Postgres, via psycopg2.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     08.05.2020
@modified    04.12.2022
------------------------------------------------------------------------------
"""
import collections
from contextlib import contextmanager
import inspect
import logging
import re
import threading

from six.moves import urllib_parse
from six import binary_type, integer_types, string_types, text_type

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
    import psycopg2.pool
except ImportError: psycopg2 = None

from .. import api, util

logger = logging.getLogger(__name__)


## Postgres reserved keywords, needing quotes in SQL queries
RESERVED_KEYWORDS = [
    "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ASC", "ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK",
    "COLLATE", "COLUMN", "CONSTRAINT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC",
    "DISTINCT", "DO", "ELSE", "END", "FALSE", "FOREIGN", "IN", "INITIALLY", "LATERAL", "LEADING",
    "LOCALTIME", "LOCALTIMESTAMP", "NOT", "NULL", "ONLY", "OR", "PLACING", "PRIMARY", "REFERENCES",
    "SELECT", "SESSION_USER", "SOME", "SYMMETRIC", "TABLE", "THEN", "TRAILING", "TRUE", "UNIQUE",
    "USER", "USING", "VARIADIC", "WHEN", "AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY",
    "CROSS", "CURRENT_SCHEMA", "FREEZE", "FULL", "ILIKE", "INNER", "IS", "JOIN", "LEFT", "LIKE",
    "NATURAL", "OUTER", "RIGHT", "SIMILAR", "TABLESAMPLE", "VERBOSE", "ISNULL", "NOTNULL",
    "OVERLAPS", "ARRAY", "AS", "CREATE", "EXCEPT", "FETCH", "FOR", "FROM", "GRANT", "GROUP",
    "HAVING", "INTERSECT", "INTO", "LIMIT", "OFFSET", "ON", "ORDER", "RETURNING", "TO", "UNION",
    "WHERE", "WINDOW", "WITH"
]


class Identifier(object):
    """Wrapper for table and column names from data objects."""
    def __init__(self, name): self.name = name
    def __eq__(self, other):
        """Supports comparing to strings or other Identifier instances."""
        if isinstance(other, type(self)): return self.name == other.name
        return isinstance(other, string_types) and (self.name == other)
    def __str__(self):
        """Returns quoted name."""
        return self.quote(self.name)
    @staticmethod
    def quote(name):
        """Returns quoted name."""
        return quote(name, force=not name.islower())  # Must quote if not lowercase


class Queryable(api.Queryable):

    ## Recognized binary operators for makeSQL()
    OPS = ("!=", "!~", "!~*", "#", "%", "&", "*", "+", "-", "/", "<", "<<",
           "<=", "<>", "<@", "=", ">", ">=", ">>", "@>", "^", "|", "||", "&&", "~",
           "~*", "ANY", "ILIKE", "IN", "IS", "IS NOT", "LIKE", "NOT ILIKE", "NOT IN",
           "NOT LIKE", "NOT SIMILAR TO", "OR", "OVERLAPS", "SIMILAR TO", "SOME")


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        sql, args = self.makeSQL("INSERT", table, values=values, kwargs=kwargs)
        cursor = self.execute(sql, args)
        row = None if cursor.description is None else next(cursor, None)
        return next(iter(row.values())) if row and isinstance(row, dict) else None


    def makeSQL(self, action, table, cols="*", where=(), group=(), order=(), limit=(), values=(),
                kwargs=None):
        """Returns (SQL statement string, parameter dict)."""

        def cast(col, val):
            """Returns column value cast to correct type for use in psycopg."""
            col = col.name if isinstance(col, Identifier) else \
                  col if isinstance(col, string_types) else \
                  self._match_name(util.nameify(col, parent=table), tablename)
            field = tablename in self._structure and self._structure[tablename]["fields"].get(col)
            if field and "array" == field["type"]:
                return list(listify(val)) # Values for array fields must be lists
            elif field and val is not None:
                val = self._adapt_value(val, field["type"])
            if isinstance(val, (list, set)):
                return tuple(val) # Sequence parameters for IN etc must be tuples
            return val

        def parse_members(i, col, op, val):
            """Returns (col, op, val, argkey)."""
            if isinstance(col, Identifier): col, colsql, pure = col.name, text_type(col), False
            elif not isinstance(col, string_types):
                col = self._match_name(util.nameify(col, parent=table), tablename)
                colsql, pure = Identifier.quote(col), False
            else: colsql, pure = col, True
            key = "%sW%s" % (re.sub(r"\W+", "_", col), i)
            if "EXPR" == col.upper() and pure:
                # ("EXPR", ("SQL", val))
                colsql, op, val, key = val[0], "EXPR", val[1], "EXPRW%s" % i
            elif col.count("?") == argcount(val) and pure:
                # ("any SQL with ? placeholders", val)
                op, val, key = "EXPR", listify(val), "EXPRW%s" % i
            elif isinstance(val, (list, tuple)) and len(val) == 2 \
            and isinstance(val[0], string_types):
                tmp = val[0].strip().upper()
                if tmp in self.OPS:
                    # ("col", ("binary op like >=", val))
                    op, val = tmp, val[1]
                elif val[0].count("?") == argcount(val[1]):
                    # ("col", ("SQL with ? placeholders", val))
                    colsql, val, op = "%s = %s" % (col, val[0]), listify(val[1]), "EXPR"
            if op in ("IN", "NOT IN") and not val: # IN -> ANY, to avoid error on empty array
                colsql = "%s%s = ANY('{}')" % ("" if "IN" == op else "NOT ", colsql)
                op = "EXPR"
            return colsql, op, cast(col, val), key
        def argcount(x)  : return len(x) if isinstance(x, (list, set, tuple)) else 1
        def listify(x)   : return x if isinstance(x, (list, tuple)) else \
                                  list(x) if isinstance(x, set) else [x]
        def keylistify(x): return x if isinstance(x, (list, tuple)) else \
                                  list(x) if isinstance(x, (dict, set)) else [x]

        def column(val, sql=False):
            """Returns column name from string/property/Identifier, quoted if object and `sql`."""
            if inspect.isdatadescriptor(val): val = util.nameify(val, wrapper(sql=sql), table)
            if isinstance(val, Identifier): return text_type(val) if sql else val.name
            return val if isinstance(val, string_types) else text_type(val)

        def wrapper(column=True, sql=False):
            """Returns function producing Identifier or SQL-ready name string."""
            def inner(name):
                val = Identifier(self._match_name(name, table=tablename if column else None))
                return text_type(val) if sql else val
            return inner

        self._load_schema()
        values0 = values
        action = action.upper()
        where, group, order, limit, values = (() if x is None else x
                                              for x in (where, group, order, limit, values))
        n = util.nameify(table, wrapper(column=False))
        tablename, tablesql = (n.name, text_type(n)) if isinstance(n, Identifier) else (n, n)
        cols   = ", ".join(util.nameify(x, wrapper(sql=True), table) for x in keylistify(cols)) or "*"
        group  = ", ".join(util.nameify(x, wrapper(sql=True), table) for x in keylistify(group))
        where  = util.keyvalues(where, wrapper())
        order  = list(order.items()) if isinstance(order, dict) else listify(order)
        order  = [order] if isinstance(order, (list, tuple)) \
                 and len(order) == 2 and isinstance(order[1], bool) else order
        limit  = [limit] if isinstance(limit, string_types + integer_types) else limit
        values = util.keyvalues(values, wrapper())
        sql    = "SELECT %s FROM %s" % (cols, tablesql) if "SELECT" == action else ""
        sql    = "DELETE FROM %s"    % (tablesql)       if "DELETE" == action else sql
        sql    = "INSERT INTO %s"    % (tablesql)       if "INSERT" == action else sql
        sql    = "UPDATE %s"         % (tablesql)       if "UPDATE" == action else sql
        args   = {}
        if kwargs and action in ("SELECT", "DELETE", "UPDATE"): where  += list(kwargs.items())
        if kwargs and action in ("INSERT", ):                   values += list(kwargs.items())

        if "INSERT" == action:
            pk = self._structure.get(tablename, {}).get("key")
            if pk and util.is_dataobject(values0):  # Can't avoid giving primary key if data object
                values = [(k, v) for k, v in values if k != pk or v is not None]  # Discard NULL pk
            keys = ["%sI%s" % (re.sub(r"\W+", "_", column(k)), i) for i, (k, _) in enumerate(values)]
            args.update((a, cast(k, v)) for i, (a, (k, v)) in enumerate(zip(keys, values)))
            cols = ", ".join(column(k, sql=True) for k, _ in values)
            vals = ", ".join("%%(%s)s" % s for s in keys)
            sql += " (%s) VALUES (%s)" % (cols, vals)
            if pk: sql += " RETURNING %s AS id" % Identifier.quote(pk)
        if "UPDATE" == action:
            sql += " SET "
            for i, (col, val) in enumerate(values):
                key = "%sU%s" % (re.sub(r"\W+", "_", column(col)), i)
                sql += (", " if i else "") + "%s = %%(%s)s" % (column(col, sql=True), key)
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

                if "EXPR" == op:
                    for j in range(col.count("?")):
                        col = col.replace("?", "%%(%s_%s)s" % (key, j), 1)
                        args["%s_%s" % (key, j)] = cast(None, val[j])
                    sql += (" AND " if i else "") + "(%s)" % col
                elif val is None:
                    op = {"=": "IS", "!=": "IS NOT", "<>": "IS NOT"}.get(op, op)
                    sql += (" AND " if i else "") + "%s %s NULL" % (col, op)
                else:
                    args[key] = val
                    sql += (" AND " if i else "") + "%s %s %%(%s)s" % (col, op, key)
        if group:
            sql += " GROUP BY " + group
        if order:
            sql += " ORDER BY "
            for i, col in enumerate(order):
                name = util.nameify(col[0] if isinstance(col, (list, tuple)) else col, quote, table)
                sort = col[1] if name != col and isinstance(col, (list, tuple)) and len(col) > 1 \
                       else ""
                if not isinstance(sort, string_types): sort = "DESC" if sort else ""
                sql += (", " if i else "") + name + (" " if sort else "") + sort
        if limit:
            limit = [None if isinstance(v, integer_types) and v < 0 else v for v in limit]
            for k, v in zip(("limit", "offset"), limit):
                if v is None: continue  # for k, v
                sql += " %s %%(%s)s" % (k.upper(), k)
                args[k] = v

        logger.log(logging.DEBUG // 2, sql)
        return sql, args


    @classmethod
    def quote(cls, value, force=False):
        """
        Returns identifier in quotes and proper-escaped for queries,
        if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

        @param   value  the value to quote, returned as-is if not string
        @param   force  whether to quote value even if not required
        """
        return quote(value, force)


    def _adapt_value(self, value, typename):
        """
        Returns value as JSON if field is a JSON type and no adapter registered for value type,
        or original value.
        """
        if typename in ("json", "jsonb") and type(value) not in self.ADAPTERS.values():
            return psycopg2.extras.Json(value, dumps=util.json_dumps)
        return value


    def _load_schema(self, force=False):
        """Populates table structure from database if uninitialized or forced."""
        if self._structure is None or force:
            self._structure = {}  # Avoid recursion on first query

            self.cursor.factory, factory0 = None, self.cursor.factory  # Ensure dict rows
            try: self._structure.update(query_schema(self, keys=True))
            finally: self.cursor.factory = factory0


    def _match_name(self, name, table=None):
        """
        Returns proper-cased name from schema lookup, or same value if no match.

        @parma   name     name of table/view or field, from data object or property
        @param   table    name of table to match field for, if not matching table name
        """
        container = self._structure.get(table, {}).get("fields") if table else self._structure
        if name not in (container or {}):  # Check for case differences
            namelc = name.lower()
            if namelc in container:        # Normal lower-case name present
                name = namelc
            elif namelc == name:           # Name from data is lowercase, check for single cased
                variants = [n for n in container if n.lower() == namelc]
                if len(variants) == 1:
                    name = variants[0]
        return name


class Database(api.Database, Queryable):
    """
    Convenience wrapper around psycopg2.ConnectionPool and Cursor.

    Queries directly on the Database object use autocommit mode.
    """

    ## Registered adapters for Python->SQL, as {typeclass: converter}
    ADAPTERS = {}

    ## Registered converters for SQL->Python pending application, as {typename: converter}
    CONVERTERS = {}

    ## Mutexes for exclusive transactions, as {Database instance: lock}
    MUTEX = collections.defaultdict(threading.RLock)

    ## Connection pool default size per Database
    POOL_SIZE = (1, 4)

    ## Connection pools, as {Database: psycopg2.pool.ConnectionPool}
    POOLS = {}


    def __init__(self, opts, **kwargs):
        """
        Creates a new Database instance for Postgres.

        By default uses a pool of 1..4 connections.

        Connection parameters can also be specified in OS environment,
        via standard Postgres environment variables like `PGUSER` and `PGPASSWORD`.

        @param   opts     Postgres connection string, or options dictionary as
                          `dict(dbname=.., user=.., password=.., host=.., port=.., ..)`
        @param   kwargs   additional arguments given to engine constructor,
                          e.g. `minconn=1, maxconn=4`
        """

        ## Data Source Name, as URL like `"postgresql://user@host/dbname"`
        self.dsn          = make_db_url(opts)
        self._kwargs      = kwargs
        self._cursor      = None
        self._cursorctx   = None
        self._txs         = []  # [Transaction, ]
        self._row_factory = None
        self._structure   = None  # Database schema as {table or view name: {"fields": {..}, ..}}


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
        """
        Executes SQL statement, returns psycopg cursor.

        @param   sql   SQL statement to execute, with psycopg-specific parameter bindings, if any
        @param   args  dictionary for %(name)s placeholders,
                       or a sequence for positional %s placeholders, or None
        """
        if not self._cursor: raise RuntimeError("Database not open.")
        self._cursor.execute(sql, args or None)
        return self._cursor


    def executescript(self, sql):
        """
        Executes the SQL as script of any number of statements.

        Reloads internal schema structure from database.

        @param   sql   script with one or more SQL statements
        """
        cursor = self.execute(sql)
        self._structure = None  # Clear database schema to force reload on next query
        return cursor


    def open(self):
        """Opens database connection if not already open."""
        if self._cursor: return
        self.init_pool(self, **self._kwargs)
        self._apply_converters()
        self._cursorctx = self.make_cursor(autocommit=True)
        self._cursor    = self._cursorctx.__enter__()


    def close(self, commit=None):
        """
        Closes the database and any pending transactions, if open.

        @param   commit  `True` for explicit commit on open transactions,
                         `False` for explicit rollback on open transactions,
                         `None` defaults to `commit` flag from transaction creations
        """
        txs, self._txs[:] = self._txs[:], []
        for tx in txs: tx.close(commit)
        if self._cursor:
            self._cursorctx.__exit__(None, None, None)
            self._cursor = None
        self._cursorctx = None
        self.MUTEX.pop(self, None)
        pool = self.POOLS.pop(self, None)
        if pool: pool.closeall()


    @property
    def closed(self):
        """Whether database connection is currently not open."""
        return not self._cursor


    @property
    def cursor(self):
        """Database engine cursor object, or `None` if closed."""
        return self._cursor


    @property
    def row_factory(self):
        """The custom row factory, if any, as `function(cursor, row tuple)`."""
        return self._row_factory


    @row_factory.setter
    def row_factory(self, row_factory):
        """
        Sets custom row factory, as `function(cursor, row tuple)`, or `None` to reset to default.

        `cursor.description` is a sequence of 7-element tuples,
        as `(name, type_code, display_size, internal_size, precision, scale, null_ok)`.
        """
        if row_factory == self._row_factory: return
        self._row_factory = row_factory
        if self._cursor: self._cursor.factory = row_factory


    def transaction(self, commit=True, exclusive=False, **kwargs):
        """
        Returns a transaction context manager.

        Context is breakable by raising Rollback.

        @param   commit     whether transaction commits at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive
                            over other Transaction instances on this Database
        @param   kwargs     engine-specific arguments, like `schema="other", lazy=True` for Postgres
        """
        tx = Transaction(self, commit, exclusive, **kwargs)
        self._txs.append(tx)
        return tx


    @contextmanager
    def make_cursor(self, commit=False, autocommit=False, schema=None, lazy=False, itersize=None):
        """
        Context manager for psycopg connection cursor.
        Creates a new cursor on an unused connection and closes it when exiting
        context, committing changes if specified.

        @param   commit      commit at the end on success
        @param   autocommit  connection autocommit mode
        @param   schema      name of Postgres schema to use, if not using default `"public"`
        @param   lazy        if true, returns a named server-side cursor that fetches rows
                             iteratively in batches; only supports making a single query
        @param   itersize    batch size in rows for server-side cursor
        @return              psycopg2 Cursor
        """
        connection = self.POOLS[self].getconn()
        try:
            connection.autocommit = autocommit
            cursor, namedcursor = None, None
            if "public" == schema: schema = None  # Default, no need to set

            # If using schema, schema tables are queried first, fallback to public.
            # Need two cursors if schema+lazy, as named cursor only does one query.
            if schema or not lazy: cursor = connection.cursor()
            if schema: cursor.execute('SET search_path TO %s,public' % quote(schema))
            if lazy: namedcursor = connection.cursor("name_%s" % id(connection))
            if lazy and itersize is not None: namedcursor.itersize = itersize

            try:
                yield namedcursor or cursor
                if commit: connection.commit()
            except GeneratorExit: pass  # Caller consumed nothing
            except Exception as e:
                if not isinstance(e, api.Rollback):
                    logger.exception("SQL error on %s:", (namedcursor or cursor).query)
                raise
            finally:
                connection.rollback()  # If not already committed, must rollback here
                try: namedcursor and namedcursor.close()
                except Exception: pass
                if schema:  # Restore default search path on this connection
                    cursor.execute("SET search_path TO public")
                    connection.commit()
                if cursor: cursor.close()
        finally: self.POOLS[self].putconn(connection)


    @classmethod
    def init_pool(cls, db, minconn=POOL_SIZE[0], maxconn=POOL_SIZE[1], **kwargs):
        """Initializes connection pool for Database if not already initialized."""
        with cls.MUTEX[db]:
            if db in cls.POOLS: return

            args = minconn, maxconn, db.dsn
            kwargs.update(cursor_factory=db._cursor_factory)
            cls.POOLS[db] = psycopg2.pool.ThreadedConnectionPool(*args, **kwargs)


    def _apply_converters(self):
        """Applies registered converters, if any, looking up type OIDs on live cursor."""
        if not self.CONVERTERS: return

        regs, self.CONVERTERS = dict(self.CONVERTERS), {}
        with self.make_cursor() as cursor:
            for typename, transformer in regs.items():
                cursor.execute("SELECT NULL::%s" % typename)
                oid = cursor.description[0][1]  # description is [(name, type_code, ..)]
                wrap = lambda x, c, f=transformer: f(x)  # psycopg invokes callback(value, cursor)
                TYPE = psycopg2.extensions.new_type((oid, ), typename, wrap)
                psycopg2.extensions.register_type(TYPE)


    def _cursor_factory(self, *args, **kwargs):
        """Returns a new RowFactoryCursor."""
        return RowFactoryCursor(self._row_factory, *args, **kwargs)


    def _notify(self, tx):
        """Notifies database of transaction closing."""
        if tx in self._txs: self._txs.remove(tx)



class Transaction(api.Transaction, Queryable):
    """
    Transaction context manager, provides convenience methods for queries.

    Supports server-side cursors; those can only be used for making a single query.

    Must be closed explicitly if not used as context manager in a with-block.
    Block can be exited early by raising Rollback.
    """

    def __init__(self, db, commit=True, exclusive=False,
                 schema=None, lazy=False, itersize=None, **__):
        """
        Creates a transaction context manager.

        Context is breakable by raising Rollback.

        @param   db         Database instance
        @param   commit     whether transaction commits automatically at exiting with-block
        @param   exclusive  whether entering a with-block is exclusive over other
                            Transaction instances Database
        @param   schema     search_path to use in this transaction
        @param   lazy       if true, uses a server-side cursor to fetch results from server
                            iteratively in batches instead of all at once,
                            supports one single query only
        @param   itersize   batch size for server-side cursor (defaults to 2000 rows)
        """
        self._db         = db
        self._lazy       = lazy
        self._cursor     = None
        self._cursorctx  = db.make_cursor(commit, schema=schema, lazy=lazy, itersize=itersize)
        self._exclusive  = exclusive
        self._exitcommit = commit
        self._enterstack = 0     # Number of levels the transaction context is nested at
        self._structure  = None  # Database schema as {table or view name: {"fields": {..}, ..}}

    def __enter__(self):
        """Context manager entry, opens cursor, returns Transaction object."""
        if self.closed: raise RuntimeError("Transaction already closed")

        if self._exclusive: Database.MUTEX[self._db].acquire()
        try:
            if not self._cursor: self._cursor = self._cursorctx.__enter__()
            self._enterstack += 1
            return self
        except Exception:
            if self._exclusive: Database.MUTEX[self._db].release()
            raise

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, closes cursor, commits or rolls back as specified on creation."""
        depth = self._enterstack = self._enterstack - 1
        try:
            if self._cursor and depth < 1:  # Last level: close properly
                self._cursorctx.__exit__(exc_type, exc_val, exc_trace)
            elif self._cursor:  # Still some depth: intermediary commit/rollback
                self.commit() if self._exitcommit and exc_type is None else self.rollback()
            return exc_type in (None, api.Rollback)
        finally:
            if depth < 1:
                self._cursor = None
                self._cursorctx = None
                self._db._notify(self)
            if self._exclusive: Database.MUTEX[self._db].release()

    def close(self, commit=None):
        """
        Closes the transaction, performing commit or rollback as specified,
        and releases database connection back to connection pool.
        Required if not using transaction as context manager in a with-block.

        @param   commit  `True` for explicit commit, `False` for explicit rollback,
                         `None` defaults to `commit` flag from creation
        """
        if not self._cursor:
            self._db._notify(self)
            return
        if commit is False: self.rollback()
        elif commit: self.commit()
        try: self._cursorctx.__exit__(None, None, None)
        finally:
            self._cursor = None
            self._cursorctx = None
            self._db._notify(self)

    def execute(self, sql, args=()):
        """
        Executes SQL statement, returns psycopg cursor.

        @param   sql   SQL statement to execute, with psycopg-specific parameter bindings, if any
        @param   args  dictionary for %(name)s placeholders,
                       or a sequence for positional %s placeholders, or None
        """
        if self.closed: raise RuntimeError("Transaction already closed")
        if not self._cursor: self._cursor = self._cursorctx.__enter__()
        self._cursor.execute(sql, args or None)
        return self._cursor

    def executescript(self, sql):
        """
        Executes the SQL as script of any number of statements.

        Reloads internal schema structure from database.

        @param   sql   script with one or more SQL statements
        """
        cursor = self.execute(sql)
        self._structure = None  # Clear database schema to force reload on next query
        return cursor

    def commit(self):
        """Commits pending actions, if any."""
        if self._cursor: self._cursor.connection.commit()

    def rollback(self):
        """Rolls back pending actions, if any."""
        if self._cursor: self._cursor.connection.rollback()

    @property
    def closed(self):
        """Whether transaction is currently not open."""
        return not self._cursorctx

    @property
    def cursor(self):
        """Database engine cursor object, or `None` if closed."""
        if not self._cursorctx: return None
        if not self._cursor: self._cursor = self._cursorctx.__enter__()
        return self._cursor

    @property
    def database(self):
        """Returns transaction Database instance."""
        return self._db

    def _load_schema(self, force=False):
        """
        Populates database table structure from database if uninitialized or forced.

        Uses parent Database for lookup if lazy cursor.
        """
        if self._lazy: return self._db._load_schema(force=force)
        return super(Transaction, self)._load_schema(force=force)



class RowFactoryCursor(psycopg2.extensions.cursor if psycopg2 else object):
    """A cursor that generates result rows via given factory callable."""

    def __init__(self, row_factory, *args, **kwargs):
        self.factory = row_factory
        super(RowFactoryCursor, self).__init__(*args, **kwargs)

    def fetchone(self):
        row = super(RowFactoryCursor, self).fetchone()
        return row if row is None else self.row_factory(row)

    def fetchmany(self, size=None):
        rows = super(RowFactoryCursor, self).fetchmany(size)
        return [self.row_factory(row) for row in rows]

    def fetchall(self):
        rows = super(RowFactoryCursor, self).fetchall()
        return [self.row_factory(row) for row in rows]

    def __next__(self): return self.row_factory(super(RowFactoryCursor, self).__next__())
    def next(self):     return self.__next__()

    def row_factory(self, row):
        """Returns value constructed with custom row factory, or `RealDictRow` if `None`."""
        if self.factory is not None:
            return self.factory(self, row)
        return psycopg2.extras.RealDictRow(zip([x[0] for x in self.description], row))



def autodetect(opts):
    """
    Returns true if input is recognizable as Postgres connection options.

    @param   opts    expected as URL string `"postgresql://user@localhost/mydb"`
                     or keyword=value format string like `"host=localhost dbname=.."`
                     or a dictionary of `dict(host="localhost", dbname=..)`
    """
    if not isinstance(opts, string_types + (dict, )): return False
    if isinstance(opts, dict):
        try: return bool(psycopg2.extensions.make_dsn(**opts) or True) # "{}" returns ""
        except Exception: return False
    try: return bool(psycopg2.extensions.parse_dsn(opts) or True) # "postgresql://" returns {}
    except Exception: return False


def make_db_url(opts):
    """Returns Postgres connection options as URL, like `"postgresql://host/dbname"`."""
    BASICS = collections.OrderedDict([("user", ""), ("password", ":"), ("host", ""),
                                      ("port", ":"), ("dbname", "/")])
    result, creds = "", False
    if isinstance(opts, string_types):
        opts = psycopg2.extensions.parse_dsn(opts)
    for i, (k, prefix) in enumerate(BASICS.items()):
        if creds and i > 1: result, creds = result + "@", False  # Either user or password set
        if opts.get(k) is not None:
            result, creds = result + prefix + "%%(%s)s" % k, (i < 2)
    result %= {k : urllib_parse.quote(text_type(opts[k])) for k in opts}
    if any(k not in BASICS for k in opts):
        result += "/" if opts.get("dbname") is None else ""
        result += "?" + urllib_parse.urlencode({k: opts[k] for k in opts if k not in BASICS})
    return "postgresql://" + result


def query_schema(queryable, keys=False, views=False, inheritance=False):
    """
    Returns database table structure populated from given database.

    @param   queryable    Database or Transaction instance
    @param   views        whether to include views
    @param   keys         whether to include primary and foreign key information
    @param   inheritance  whether to include parent-child table information
                          and populate inherited foreign keys
    @return  ```{table or view name: {
                     "fields": OrderedDict({
                         column name: {
                             "name": column name,
                             "type": column type name,
                             ?"pk":  True,
                             ?"fk":  foreign table name,
                         }
                     }),
                     ?"key":      primary key column name,
                     ?"parent":   parent table name,
                     ?"children": [child table name, ],
                     "type":      "table" or "view",
                 }
             }```
    """
    result = {}

    # Retrieve column names
    for v in queryable.fetchall("information_schema.columns", table_schema="public",
                                order="table_name, ordinal_position"):
        t, c, d = v["table_name"], v["column_name"], v["data_type"]
        if t not in result: result[t] = {"type": "table",
                                         "fields": collections.OrderedDict()}
        result[t]["fields"][c] = {"name": c, "type": d.lower()}

    # Retrieve primary and foreign keys
    for v in queryable.fetchall(
        "information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
          "ON tc.constraint_name = kcu.constraint_name "
        "JOIN information_schema.constraint_column_usage ccu "
          "ON ccu.constraint_name = tc.constraint_name ",
        cols="DISTINCT tc.table_name, kcu.column_name, tc.constraint_type, "
        "ccu.table_name AS table_name2", where={"tc.table_schema": "public"}
    ) if keys else ():
        t, c, t2 = v["table_name"], v["column_name"], v["table_name2"]
        if "PRIMARY KEY" == v["constraint_type"]:
            result[t]["fields"][c]["pk"], result[t]["key"] = True, c
        else: result[t]["fields"][c]["fk"] = t2

    # Retrieve inheritance information, copy foreign key flags from parent
    for v in queryable.fetchall(
        "information_schema.pg_inherits i JOIN information_schema.pg_class c ON inhrelid=c.oid "
        "JOIN information_schema.pg_class p ON inhparent = p.oid "
        "JOIN information_schema.pg_namespace pn ON pn.oid = p.relnamespace "
        "JOIN information_schema.pg_namespace cn "
          "ON cn.oid = c.relnamespace AND cn.nspname = pn.nspname",
        cols="c.relname AS child, p.relname AS parent",
        where={"pn.nspname": "public"}
    ) if inheritance else ():
        result[v["parent"]].setdefault("children", []).append(v["child"])
        result[v["child"]]["parent"] = v["parent"]
        for f, opts in result[v["parent"]]["fields"].items() if keys else ():
            if not opts.get("fk"): continue  # for f, opts
            result[v["child"]]["fields"][f]["fk"] = opts["fk"]

    # Retrieve view column names
    for v in queryable.fetchall(
        "information_schema.pg_attribute a "
        "JOIN information_schema.pg_class c ON a.attrelid = c.oid "
        "JOIN information_schema.pg_namespace s ON c.relnamespace = s.oid "
        "JOIN information_schema.pg_type t ON a.atttypid = t.oid "
        "JOIN information_schema.pg_proc p ON t.typname = p.proname ",
        cols="DISTINCT c.relname, a.attname, pg_get_function_result(p.oid) AS data_type",
        where={"a.attnum": (">", 0), "a.attisdropped": False,
               "s.nspname": "public", "c.relkind": ("IN", ("v", "m"))}
    ) if views else ():
        t, c, d = v["relname"], v["attname"], v["data_type"]
        if t not in result: result[t] = {"type": "view",
                                         "fields": collections.OrderedDict()}
        result[t]["fields"][c] = {"name": c, "type": d.lower()}

    return result


def quote(value, force=False):
    """
    Returns identifier in quotes and proper-escaped for queries,
    if value needs quoting (has non-alphanumerics, starts with number, or is reserved).

    @param   value  the value to quote, returned as-is if not string
    @param   force  whether to quote value even if not required
    """
    if not isinstance(value, string_types):
        return value
    RGX_INVALID, RGX_UNICODE = r"(^[\W\d])|(?=\W)", r"[^\x01-\x7E]"
    result = value.decode() if isinstance(value, binary_type) else value
    if force or result.upper() in RESERVED_KEYWORDS or re.search(RGX_INVALID, result):
        if re.search(RGX_UNICODE, value):  # Convert to Unicode escape U&"\+ABCDEF"
            result = result.replace("\\", r"\\").replace('"', '""')
            result = 'U&"%s"' % re.sub(RGX_UNICODE, lambda m: r"\+%06X" % ord(m.group(0)), value)
        else:
            result = '"%s"' % result.replace('"', '""')
    return result


def register_adapter(transformer, typeclasses):
    """Registers function to auto-adapt given Python types to Postgres types in query parameters."""
    def adapt(x):
        """Wraps transformed value in psycopg protocol object."""
        v = transformer(x)
        return psycopg2.extensions.AsIs(v if isinstance(v, binary_type) else text_type(v).encode())

    for t in typeclasses:
        psycopg2.extensions.register_adapter(t, adapt)
        Database.ADAPTERS[t] = transformer


def register_converter(transformer, typenames):
    """Registers function to auto-convert given Postgres types to Python types in query results."""
    typenames = [n.upper() for n in typenames]
    if "JSON" in typenames:
        psycopg2.extras.register_default_json(globally=True, loads=transformer)
    if "JSONB" in typenames:
        psycopg2.extras.register_default_jsonb(globally=True, loads=transformer)
    Database.CONVERTERS.update({n: transformer for n in typenames if n not in ("JSON", "JSONB")})



if psycopg2:
    try:
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
    except Exception: logger.exception("Error configuring psycopg.")


__all__ = [
    "RESERVED_KEYWORDS", "Database", "Transaction",
    "autodetect", "quote", "register_adapter", "register_converter",
]
