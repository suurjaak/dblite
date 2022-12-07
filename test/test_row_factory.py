#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test custom row factory.

Running Postgres test needs sufficient variables in environment like `PGUSER`.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     04.12.2022
@modified    06.12.2022
------------------------------------------------------------------------------
"""
import collections
import contextlib
import logging
import os
import tempfile
import unittest

import dblite

logger = logging.getLogger()


class TestRowFactory(unittest.TestCase):
    """Tests row factories."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   (":memory:", {}),
        "postgres": ({}, {"maxconn": 2}),
    }

    ## Table columns as {table name: [{"name", "type"}]}
    TABLES = {
        "test": [{"name": "id",  "type": "INTEGER PRIMARY KEY"},
                 {"name": "val", "type": "TEXT"}],
    }

    ## Table test data, as {table name: [{row}]}
    DATAS = {
        "test": [
            {"id": 1, "val": u"val1"},
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TestRowFactory, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._connections = collections.OrderedDict()  # {engine: (opts, kwargs)}
        self._path = None  # Path to SQLite database


    def setUp(self):
        """Creates engine connection options."""
        super(TestRowFactory, self).setUp()
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as f: self._path = f.name
        self._connections["sqlite"] = (self._path, self.ENGINES["sqlite"][1])

        try: import psycopg2
        except ImportError:
            logger.warning("Skip testing postgres, psycopg2 not available.")
            return
        opts, kwargs = self.ENGINES["postgres"]
        try:
            dblite.init(opts, **kwargs).close()
        except psycopg2.Error as e:
            logger.warning("Skip testing postgres, connection failed with:\n%s", e)
        else:
            self._connections["postgres"] = (opts, kwargs)
        dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def tearDown(self):
        """Deletes temoorary files and tables."""
        try: os.remove(self._path)
        except Exception: pass
        try:
            opts, kwargs = self._connections["postgres"]
            with dblite.init(opts, "postgres", **kwargs) as db:
                for table in self.TABLES: db.executescript("DROP TABLE IF EXISTS %s" % table)
        except Exception: pass
        super(TestRowFactory, self).tearDown()


    def test_row_factory(self):
        """Tests row factories."""
        logger.info("Verifying row factories.")
        for i, (engine, (opts, kwargs)) in enumerate(self._connections.items()):
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases
            with self.subTest(engine) if hasattr(self, "subTest") else contextlib.nested():  # Py3/2
                if i: logger.info("-" * 60)
                dblite.init(opts, **kwargs)
                self.verify_row_factory(engine)
                dblite.close()


    def verify_row_factory(self, engine):
        """Verifies setting and clearing row factory."""
        logger.info("Verifying row factory for %s.", engine)
        dblite.register_row_factory(str_factory)
        for table, cols in self.TABLES.items():
            dblite.executescript("DROP TABLE IF EXISTS %s" % table)
            dblite.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))
            for data in self.DATAS[table]:
                dblite.insert(table, data)

        logger.info("Verifying registering global row factory on open connection.")
        for i, factory in enumerate([kv_factory, None]):
            logger.debug("Verifying %s.", factory.__name__ if factory else "resetting row factory")
            dblite.register_row_factory(factory, engine=engine if i else None)
            for table, datas in self.DATAS.items():
                for data in datas:
                    expected = data if factory is None else \
                               [(c["name"], data[c["name"]]) for c in self.TABLES[table]]
                    self.assertEqual(dblite.fetchone(table), expected,
                                     "Unexpected value from dblite.fetchone().")

        logger.info("Verifying registering global row factory on closed connection.")
        for i, factory in enumerate([kv_factory, None]):
            dblite.close()
            logger.debug("Verifying %s.", factory.__name__ if factory else "resetting row factory")
            dblite.register_row_factory(factory, engine=engine if i else None)
            dblite.init()
            for table, datas in self.DATAS.items():
                for i, row in enumerate(dblite.fetchall(table, order="id")):
                    expected = datas[i] if factory is None else \
                               [(c["name"], datas[i][c["name"]]) for c in self.TABLES[table]]
                    self.assertEqual(row, expected, "Unexpected value from dblite.fetchall().")

        logger.info("Verifying registering row factory on Database.")
        dblite.close()
        dblite.register_row_factory(kv_factory)
        db = dblite.init()
        for factory in (str_factory, None):
            logger.debug("Verifying %s on Database.",
                         factory.__name__ if factory else "resetting row factory")
            db.row_factory = factory
            for table, datas in self.DATAS.items():
                for i, row in enumerate(db.select(table, order="id")):
                    expected = datas[i] if factory is None else \
                               str(tuple(datas[i][c["name"]] for c in self.TABLES[table]))
                    self.assertEqual(row, expected, "Unexpected value from Database.select().")
                logger.debug("Verifying row factory on Transaction.")
                with db.transaction() as tx:
                    for i, row in enumerate(tx.select(table, order="id")):
                        expected = datas[i] if factory is None else \
                                   str(tuple(datas[i][c["name"]] for c in self.TABLES[table]))
                        self.assertEqual(row, expected, "Unexpected value from Transaction.select().")


def kv_factory(cursor, row):
    """Returns row as [(column name, column value), ]."""
    return list(zip([c[0] for c in cursor.description], row))


def str_factory(cursor, row):
    """Returns row as str(row)."""
    return str(row)



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_row_factory] %(message)s"
    )
    unittest.main()
