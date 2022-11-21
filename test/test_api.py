#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test database general API in available engines.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     20.11.2022
@modified    20.11.2022
------------------------------------------------------------------------------
"""
import collections
import copy
import logging
import os
import tempfile
import unittest

import dblite

logger = logging.getLogger()


class TestAPI(unittest.TestCase):
    """Tests dblite API."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   ({}, {}),
        "postgres": ({"user": "postgres", "host": "localhost"}, {"maxconn": 2}),
    }

    ## Table columns as {table name: [{"name", "type"}]}
    TABLES = {
        "test": [{"name": "id",  "type": "INTEGER PRIMARY KEY"},
                 {"name": "val", "type": "TEXT"}],
    }

    ## Table test data, as {table name: [{row}]}
    DATAS = {
        "test": [
            {"id": 1, "val": "val1"},
            {"id": 2, "val": "val2"},
            {"id": 3, "val": "val3"},
            {"id": 4, "val": "val4"},
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TestAPI, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._connections = collections.OrderedDict()  # {engine: (opts, kwargs)}
        self._path = None  # Path to SQLite database


    def setUp(self):
        """Creates engine connection options."""
        super(TestAPI, self).setUp()
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as f: self._path = f.name
        self._connections["sqlite"] = (self._path, self.ENGINES["sqlite"][1])

        try: import psycopg2
        except ImportError:
            logger.warning("Skip testing postgres, psycopg2 not available.")
            return
        opts, kwargs = self.ENGINES["postgres"]
        try:
            dblite.init(opts, **kwargs)
        except psycopg2.Error as e:
            logger.warning("Skip testing postgres, connection failed with:\n%s", e)
        else:
            self._connections["postgres"] = (opts, kwargs)
        dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def tearDown(self):
        """Deletes temoorary files."""
        try: os.remove(self._path)
        except Exception: pass
        super(TestAPI, self).tearDown()


    def test_api(self):
        """Tests dblite API."""
        logger.info("Verifying dblite API.")
        for engine, (opts, kwargs) in self._connections.items():
            self.verify_module_api(opts, kwargs, engine)
            self.verify_query_api(dblite, engine)
            dblite.close()
            with dblite.init() as db:
                self.verify_query_api(db, engine)
            with dblite.transaction() as tx:
                self.verify_query_api(tx, engine)
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def verify_query_api(self, obj, engine):
        """Verifies query functions."""
        logger.info("Verifying %s query functions for %s.", label(obj), engine)
        DATAS = copy.deepcopy(self.DATAS)

        for table, cols in self.TABLES.items():
            obj.executescript("DROP TABLE IF EXISTS %s" % table)
            obj.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))
            logger.debug("Verifying %s.insert(%s).", label(obj), table)
            for i, data in enumerate(DATAS[table]):
                myid = obj.insert(table, data) if i % 2 else obj.insert(table, **data)
                self.assertEqual(myid, data["id"], "Unexpected value from %s.insert()." % obj)
            logger.debug("Verifying %s.fetchone(%s).", label(obj), table)
            for i, data in enumerate(DATAS[table]):
                row = obj.fetchone(table, id=data["id"]) if i % 2 else obj.fetchone(table, where=data)
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
            logger.debug("Verifying %s.fetchall(%r).", label(obj), table)
            rows = obj.fetchall(table)
            self.assertEqual(rows, DATAS[table], "Unexpected value from %s.fetchall()." % obj)

            logger.debug("Verifying %s.update(%r).", label(obj), table)
            for i, data in enumerate(DATAS[table]):
                data.update(val=data["val"] * 3)  # Update DATAS
                affected = obj.update(table, data, id=data["id"]) if i % 2 else \
                           obj.update(table, data, {"id": data["id"]})
                self.assertEqual(affected, 1, "Unexpected value from %s.update()." % obj)
                row = obj.fetchone(table, id=data["id"])
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
        if isinstance(obj, dblite.api.Queryable):
            obj.close()
        if isinstance(obj, dblite.api.Database):
            obj.open()
        if isinstance(obj, dblite.api.Transaction):
            obj = obj.database.transaction()  # Create new transaction for verifying persistence

        logger.debug("Verifying %s persistence.", label(obj))
        for table, cols in self.TABLES.items():
            for i, data in enumerate(DATAS[table]):
                row = obj.fetchone(table, id=data["id"]) if i % 2 else obj.fetchone(table, where=data)
                #print(row, data)
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
            rows = obj.fetchall(table)
            self.assertEqual(rows, DATAS[table], "Unexpected value from %s.fetchall()." % obj)

            logger.debug("Verifying %s.delete(%r).", label(obj), engine)
            for i, data in enumerate(DATAS[table][::2]):
                affected = obj.delete(table, id=data["id"]) if i % 2 else \
                           obj.delete(table, data)
                self.assertEqual(affected, 1, "Unexpected value from %s.delete()." % obj)
                row = obj.fetchone(table, id=data["id"])
                self.assertIsNone(row, "Unexpected value from %s.fetchone()." % obj)

            logger.debug("Verifying %s.select(%r).", label(obj), table)
            rows = list(obj.select(table))
            self.assertGreater(rows, [], "Unexpected value from %s.fetchall()." % obj)

            affected = obj.delete(table)
            self.assertGreater(affected, 1, "Unexpected value from %s.delete()." % obj)

            logger.debug("Verifying %s.executescript().", label(obj))
            obj.executescript("DROP TABLE %s" % table)
            with self.assertRaises(Exception,
                                   msg="Unexpected success for fetch after dropping table."):
                obj.fetchone(table)

        if isinstance(obj, dblite.api.Transaction):
            obj.close()


    def verify_module_api(self, opts, kwargs, engine):
        """Verifies general module-level functions."""
        logger.info("Verifying dblite module-level functions for %s.", engine)

        logger.debug("Verifying dblite.init().")
        db = dblite.init(opts, engine=engine, **kwargs)
        self.assertIsInstance(db, dblite.api.Database, "Unexpected value from dblite.init().")
        db2 = dblite.init()
        self.assertIs(db2, db, "Unexpected value from dblite.init().")

        logger.debug("Verifying dblite.transaction().")
        tx = db.transaction()
        self.assertIsInstance(tx, dblite.api.Transaction,
                              "Unexpected value from dblite.transaction().")
        tx.close()

        logger.debug("Verifying dblite.close().")
        self.assertFalse(db.closed, "Unexpected value from Database.closed.")
        dblite.close()
        self.assertTrue(db.closed, "Unexpected value from Database.closed.")
        with self.assertRaises(Exception,
                               msg="Unexpected success for fetch after closing database."):
            db.fetchone(next(iter(self.TABLES)))


def label(obj):
    """Returns readable name for logging, for `dblite` module or class instances."""
    if isinstance(obj, dblite.api.Queryable):
        return "%s.%s" % (obj.__class__.__module__, obj.__class__.__name__)
    return "dblite"


if "__main__" == __name__:
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(levelname)s]\t[%(created).06f] [test_api] %(message)s"
    )
    unittest.main()
