# -*- coding: utf-8 -*-
"""
Database connectivity. Transaction usage:

    with Transaction() as tx:
        tx.execute("CREATE TABLE test (id BIGSERIAL PRIMARY KEY, val TEXT)")

        # Keyword arguments are added to WHERE clause,
        # or to VALUES clause for INSERT:

        tx.insert("test", val=None)
        for i in range(5):
            tx.insert("test", {"val": i})
        tx.fetchone("test", id=1)
        tx.fetchall("test", order="val", limit=3)
        tx.update("test", {"val": "ohyes"}, id=5)
        tx.fetchone("test", val="ohyes")
        tx.delete("test", val="ohyes")

        # WHERE clause supports simple equality match, binary operators,
        # collection lookups ("IN", "NOT IN"), or arbitrary SQL strings.
        # Arbitrary SQL parameters expect "?" placeholders.
        # Argument for key-value parameters, like WHERE or VALUES,
        # can be a dict, or a sequence of key-value pairs:

        tx.fetchall("test", val="ciao")
        tx.fetchall("test", where={"id": ("<", 10)})
        tx.fetchall("test", id=("IN", range(5)))
        tx.fetchall("test", val=("IS NOT", None))
        tx.update("test", values={"val": "ohyes"}, where=[("id", 1)])
        tx.fetchall("test", where=[("LENGTH(val)", ">", 4), ])
        tx.fetchall("test", where=[("LENGTH(val) < ?", 4), ])

        # WHERE arguments are ANDed together, OR needs subexpressions:

        tx.fetchall("test", where=[("id < ? OR id > ?", [2, 3]), ("val", 3)])

        # Argument for sequence parameters, like GROUP BY, ORDER BY, or LIMIT,
        # can be an iterable sequence like list or tuple, or a single value:

        tx.fetchall("test", group="val", order=["id", ("val", False)], limit=3)
        tx.fetchall("test", limit=(10, 100)) # LIMIT 10 OFFSET 100

        tx.execute("DROP TABLE test")


    # Supports server-side cursors for iterative data access,
    # not fetching and materializing all rows at once:

    with Transaction(lazy=True) as tx:
        for i, row in enumerate(tx.select("some really huge table")):
            print "Processing row #%s" % i


    # Raising Rollback will exit the context manager without raising upward:

    with Transaction(commit=True) as tx:
        if not tx.fetchone("fafafa", "1"):
            raise Rollback
        tx.delete("fafafa")  # Will not be reached if table is empty
    print "great success"    # Will print, raised Rollback only breaks with-block


------------------------------------------------------------------------------
This file is part of dblite - simple query interface to SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     08.05.2020
@modified    17.11.2022
"""
from collections import OrderedDict
from contextlib import contextmanager
import logging
import re

from six import binary_type, integer_types, string_types, text_type

try:
    import psycopg2
    import psycopg2.extensions
    import psycopg2.extras
    import psycopg2.pool
except ImportError: psycopg2 = None

from . import Database as DB, Queryable as QQ, Rollback, Transaction as TX
from . import json_dumps

logger = logging.getLogger(__name__)


class Queryable(QQ):

    TABLES = {}  # {opts+kwargs str: table structure filled on first access}
    # {name: {key: "pk", fields: {col: {name, type, ?fk: "t2"}},
    #         ?parent: "t3", ?children: ("t4", ), ?type: "view"}}

    # Recognized binary operators for makeSQL
    OPS = ("!=", "!~", "!~*", "#", "%", "&", "*", "+", "-", "/", "<", "<<",
           "<=", "<>", "<@", "=", ">", ">=", ">>", "@>", "^", "|", "||", "&&", "~",
           "~*", "ANY", "ILIKE", "IN", "IS", "IS NOT", "LIKE", "NOT ILIKE", "NOT IN",
           "NOT LIKE", "NOT SIMILAR TO", "OR", "OVERLAPS", "SIMILAR TO", "SOME")


    def makeSQL(self, action, table, cols="*", where=(), group=(), order=(),
                limit=(), values=()):
        """Returns (SQL statement string, parameter dict)."""
        key = self._key if isinstance(self, Database) else self._db._key
        if key not in self.TABLES:
            self.TABLES[key] = {}  # To skip later if querying fails
            self.TABLES[key].update(self.query_schema(keys=True))
        TABLES = self.TABLES[key]

        def cast(col, val):
            """Returns column value cast to correct type for use in psycopg."""
            field = table in TABLES and TABLES[table]["fields"].get(col)
            if field and "array" == field["type"]:
                return list(listify(val)) # Values for array fields must be lists
            elif field and val is not None:
                return self.adapt_value(val, field["type"])
            if isinstance(val, (list, set)):
                return tuple(val) # Sequence parameters for IN etc must be tuples
            return val

        def parse_members(i, col, op, val):
            """Returns (col, op, val, argkey)."""
            key = "%sW%s" % (re.sub("\\W+", "_", col), i)
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
            if op in ("IN", "NOT IN") and not val: # IN -> ANY, to avoid error on empty array
                col = "%s%s = ANY('{}')" % ("" if "IN" == op else "NOT ", col)
                op = "EXPR"
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
            args.update((k, cast(k, v)) for k, v in values)
            cols, vals = (", ".join(x % k for k, v in values) for x in ("%s", "%%(%s)s"))
            sql += " (%s) VALUES (%s)" % (cols, vals)
            if TABLES and table in TABLES and TABLES[table].get("key"):
                sql += " RETURNING %s AS id" % (TABLES[table]["key"])
        if "UPDATE" == action:
            sql += " SET "
            for i, (col, val) in enumerate(values):
                sql += (", " if i else "") + "%s = %%(%sU%s)s" % (col, col, i)
                args["%sU%s" % (col, i)] = cast(col, val)
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
                    args[key] = cast(col, val)
                    sql += (" AND " if i else "") + "%s %s %%(%s)s" % (col, op, key)
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
            sql += " %s %%(%s)s" % (k.upper(), k)
            args[k] = v

        return sql, args


    def query_schema(self, keys=False, views=False, inheritance=False):
        """
        Returns database table structure populated from current connection.

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

        db = self if isinstance(self, DB) else self._db
        with Transaction(db, schema="information_schema") as tx:
            # Retrieve column names
            for v in tx.fetchall("columns", table_schema="public",
                                 order="table_name, dtd_identifier"):
                t, c, d = v["table_name"], v["column_name"], v["data_type"]
                if t not in result: result[t] = {"type": "table", "fields": OrderedDict()}
                result[t]["fields"][c] = {"name": c, "type": d.lower()}

            # Retrieve primary and foreign keys
            for v in tx.fetchall(
                "table_constraints tc JOIN key_column_usage kcu "
                  "ON tc.constraint_name = kcu.constraint_name "
                "JOIN constraint_column_usage ccu "
                  "ON ccu.constraint_name = tc.constraint_name ",
                cols="DISTINCT tc.table_name, kcu.column_name, tc.constraint_type, "
                "ccu.table_name AS table_name2", where={"tc.table_schema": "public"}
            ) if keys else ():
                t, c, t2 = v["table_name"], v["column_name"], v["table_name2"]
                if "PRIMARY KEY" == v["constraint_type"]:
                    result[t]["fields"][c]["pk"], result[t]["key"] = True, c
                else: result[t]["fields"][c]["fk"] = t2

            # Retrieve inheritance information, copy foreign key flags from parent
            for v in tx.fetchall(
                "pg_inherits i JOIN pg_class c ON inhrelid=c.oid "
                "JOIN pg_class p ON inhparent = p.oid "
                "JOIN pg_namespace pn ON pn.oid = p.relnamespace "
                "JOIN pg_namespace cn "
                  "ON cn.oid = c.relnamespace AND cn.nspname = pn.nspname",
                cols="c.relname AS child, p.relname AS parent",
                where={"pn.nspname": "public"}
            ) if inheritance else ():
                result[v["parent"]].setdefault("children", []).append(v["child"])
                result[v["child"]]["parent"] = v["parent"]
                for f, opts in result[v["parent"]]["fields"].items() if keys else ():
                    if not opts.get("fk"): continue # for f, opts
                    result[v["child"]]["fields"][f]["fk"] = opts["fk"]

            # Retrieve view column names
            for v in tx.fetchall(
                "pg_attribute a "
                "JOIN pg_class c ON a.attrelid = c.oid "
                "JOIN pg_namespace s ON c.relnamespace = s.oid "
                "JOIN pg_type t ON a.atttypid = t.oid "
                "JOIN pg_proc p ON t.typname = p.proname ",
                cols="DISTINCT c.relname, a.attname, pg_get_function_result(p.oid) AS data_type",
                where={"a.attnum": (">", 0), "a.attisdropped": False,
                       "s.nspname": "public", "c.relkind": ("IN", ("v", "m"))}
            ) if views else ():
                t, c, d = v["relname"], v["attname"], v["data_type"]
                if t not in result: result[t] = {"fields": OrderedDict(), "type": "view"}
                result[t]["fields"][c] = {"name": c, "type": d.lower()}
        return result


    def adapt_value(self, value, typename):
        """
        Returns value as JSON if field is a JSON type and no adapter registered for value type,
        or original value.
        """
        if typename in ("json", "jsonb") and type(value) not in self.ADAPTERS.values():
            return psycopg2.extras.Json(value, dumps=json_dumps)
        return value



class Database(DB, Queryable):
    """Convenience wrapper around psycopg2.ConnectionPool and Cursor."""

    ## Connection pools, as {opts+kwargs str: psycopg2.pool.ConnectionPool}
    POOLS = {}

    ## Registered adapters for Python->SQL, as {typeclass: converter}
    ADAPTERS = {}

    ## Registered converters for SQL->Python pending application, as {typename: converter}
    CONVERTERS = {}


    @classmethod
    def init_pool(cls, key, opts, minconn=1, maxconn=4, **kwargs):
        """Initializes connection pool if not already initialized."""
        if key in cls.POOLS: return

        args = dict(minconn=minconn, maxconn=maxconn)
        dsn = opts if isinstance(opts, string_types) else None
        args.update(opts if isinstance(opts, dict) else {}, **kwargs)
        cls.POOLS[key] = psycopg2.pool.ThreadedConnectionPool(dsn, **args)


    def __init__(self, opts, **kwargs):
        """
        Creates a new Database instance for Postgres.

        By default uses a pool of 1..4 connections.

        Connection parameters can also be specified in OS environment,
        standard Postgres environment variables like `PGUSER` and `PGPASSWORD`.

        @param   opts     Postgres connection string, or options dictionary as
                          `dict(dbname=None, username=None, password=None,
                                host=None, port=None, minconn=1, maxconn=4, ..)`
        @param   kwargs   additional arguments given to engine constructor,
                          e.g. `minconn=1, maxconn=4`
        """
        self._key       = str(opts) + str(kwargs)
        self._opts      = opts
        self._kwargs    = kwargs
        self._cursor    = None
        self._cursorctx = None


    @contextmanager
    def get_cursor(self, commit=True, schema=None, lazy=False):
        """
        Context manager for psycopg connection cursor.
        Creates a new cursor on an unused connection and closes it when exiting
        context, committing changes if specified.

        @param   commit  auto-commit at the end on success
        @param   schema  name of Postgres schema to use, if not using default public
        @param   lazy    if true, returns a named cursor that fetches rows
                         iteratively; only supports making a single query
        @return          psycopg2.extras.RealDictCursor
        """
        connection = self.POOLS[self._key].getconn()
        try:
            cursor, namedcursor = None, None
            if "public" == schema: schema = None  # Default, no need to set

            # If using schema, schema tables are queried first, fallback to public.
            # Need two cursors if schema+lazy, as named cursor only does one query.
            if schema or not lazy: cursor = connection.cursor()
            if schema: cursor.execute('SET search_path TO "%s",public' % schema)
            if lazy: namedcursor = connection.cursor("name_%s" % id(connection))

            try:
                yield namedcursor or cursor
                if commit: connection.commit()
            except GeneratorExit: pass  # Caller consumed nothing
            except Exception:
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
        finally: self.POOLS[self._key].putconn(connection)


    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        values = list(values.items() if isinstance(values, dict) else values)
        values += kwargs.items()
        sql, args = self.makeSQL("INSERT", table, values=values)
        res = next(self.execute(sql, args))
        return res.values()[0] if res and isinstance(res, dict) else None


    def execute(self, sql, args=()):
        """
        Executes SQL statement, returns psycopg cursor.

        @param   args  dictionary for %(name)s placeholders,
                       or a sequence for positional %s placeholders, or None
        """
        if not self._cursorctx: self._cursorctx = self.get_cursor(commit=True)
        if not self._cursor:    self._cursor = self._cursorctx.__enter__()
        self._cursor.execute(sql, args or None)
        return self._cursor


    def executescript(self, sql):
        """Executes the SQL as script of any number of statements."""
        return self.execute(sql)


    def open(self):
        """Opens database connection if not already open."""
        if self._cursorctx: return
        self.init_pool(self._key, self._opts, **self._kwargs)
        self.apply_converters()
        self._cursorctx = self.get_cursor(commit=True)


    def close(self):
        """Closes connection."""
        if self._cursor:
            self._cursorctx.__exit__(None, None, None)
            self._cursor = None
        self._cursorctx = None
        pool = self.POOLS.pop(self._key, None)
        if pool: pool.close_all()


    @classmethod
    def register_converter(cls, transformer, typenames):
        """
        Registers function to auto-convert given SQL types to Python in query results.

        Will be applied as soon as a cursor is created, as type OIDS need lookup.
        """
        cls.CONVERTERS.update({n: transformer for n in typenames})


    def apply_converters(self):
        """Applies registered converters, if any, looking up type OIDs on cursor."""
        if not self.CONVERTERS: return

        regs, self.CONVERTERS = dict(self.CONVERTERS), {}
        with self.get_cursor() as cursor:
            for typename, transformer in regs.items():
                cursor.execute("SELECT NULL::%s" % typename)
                oid = cursor.description[0][1]  # description is [(name, type_code, ..)]
                wrap = lambda x, c, f=transformer: f(x)  # psycopg invokes callback(value, cursor)
                TYPE = psycopg2.extensions.new_type((oid, ), typename, wrap)
                psycopg2.extensions.register_type(TYPE)


class Transaction(TX, Queryable):
    """
    Transaction context manager, provides convenience methods for queries.
    Supports lazy cursors; those can only be used for making a single query.
    Must be closed explicitly if not used as context manager in a with-block.
    Block can be exited early by raising Rollback.
    """

    def __init__(self, db, commit=True, schema=None, lazy=False):
        """
        @param   commit   if true, transaction auto-commits at the end
        @param   schema   search_path to use in this transaction
        @param   lazy     if true, fetches results from server iteratively
                          instead of all at once, supports single query only
        """
        super(Transaction, self).__init__(db, commit)
        self._cursor = None
        self._cursorctx = db.get_cursor(commit, schema, lazy)

    def __enter__(self):
        """Context manager entry, returns Transaction object."""
        self._cursor = self._cursorctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_trace):
        """Context manager exit, propagates raised errors except Rollback."""
        self._cursorctx.__exit__(exc_type, exc_val, exc_trace)
        self._cursor = None
        return exc_type in (None, Rollback)

    def close(self, commit=None):
        """
        Closes the transaction, performing commit or rollback as configured,
        and releases database connection back to connection pool.
        Required if not using transaction as context manager in a with-block.

        @param   commit  if true, performs explicit final commit on transaction;
                         if false, performs explicit rollback
        """
        if self._cursor:
            if commit is False: self.rollback()
            elif commit:        self.commit()
            self.__exit__(None, None, None)

    def insert(self, table, values=(), **kwargs):
        """
        Convenience wrapper for database INSERT, returns inserted row ID.
        Keyword arguments are added to VALUES.
        """
        values = list(values.items() if isinstance(values, dict) else values)
        values += kwargs.items()
        sql, args = self._db.makeSQL("INSERT", table, values=values)
        res = next(self.execute(sql, args))
        return res.values()[0] if res and isinstance(res, dict) else None

    def execute(self, sql, args=()):
        """
        Executes SQL statement, returns psycopg cursor.

        @param   args  dictionary for %(name)s placeholders,
                       or a sequence for positional %s placeholders, or None
        """
        if not self._cursor: self._cursor = self._cursorctx.__enter__()
        self._cursor.execute(sql, args or None)
        return self._cursor

    def executescript(self, sql):
        """Executes the SQL as script of any number of statements."""
        return self.execute(sql)

    def commit(self):
        """Commits current transaction, if any."""
        if self._cursor: self._cursor.connection.commit()

    def rollback(self):
        """Rolls back current transaction, if any."""
        if self._cursor: self._cursor.connection.rollback()


def autodetect(opts):
    """
    Returns true if inputs are recognizable as Postgres connection options.

    @param   opts    expected as URL string `"postgresql://user@localhost/mydb"`
                     or keyword=value format string like `"host=localhost dbname=.."`
                     or a dictionary of `dict(host="localhost", dbname=..)`
    """
    if isinstance(opts, dict):
        return bool(opts.get("dbname"))
    elif isinstance(opts, string_types):
        return opts.startswith("postgresql://") or bool(re.match(r"\w+=\S*", opts))
    return False


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
    """Registers function to auto-convert given SQL types to Python in query results."""
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
