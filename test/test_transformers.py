#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test adapters and converters.

Running Postgres test needs sufficient variables in environment like `PGUSER`.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     22.11.2022
@modified    18.04.2023
------------------------------------------------------------------------------
"""
import collections
import contextlib
import datetime
import json
import logging
import unittest

import dblite

logger = logging.getLogger()


class TestTransformers(unittest.TestCase):
    """Tests adapters and converters."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   (":memory:", {}),
        "postgres": ({}, {"maxconn": 2}),
    }

    ## Table columns as {table name: [{"name", "type"}]}
    TABLES = {
        "test": [{"name": "id",  "type": "INTEGER PRIMARY KEY"},
                 {"name": "dt",  "type": "TIMESTAMP"},
                 {"name": "val", "type": "JSON"}],
    }

    ## Table test data, as {table name: [{row}]}
    DATAS = {
        "test": [
            {"id": 1, "dt": datetime.datetime.now(dblite.util.UTC),
             "val": {"nested": {"value": [1, 2]}}},
            {"id": 2, "dt": datetime.datetime.now(dblite.util.UTC),
             "val": {"nested": [None, False, 1.1, 2.2]}},
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TestTransformers, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._connections = collections.OrderedDict()  # {engine: (opts, kwargs)}


    def setUp(self):
        """Creates engine connection options."""
        super(TestTransformers, self).setUp()
        self._connections["sqlite"] = self.ENGINES["sqlite"]

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
        """Drops created tables from Postgres."""
        try:
            opts, kwargs = self._connections["postgres"]
            with dblite.init(opts, "postgres", **kwargs) as db:
                for table in self.TABLES: db.executescript("DROP TABLE IF EXISTS %s" % table)
        except Exception: pass
        super(TestTransformers, self).tearDown()


    def test_transformers(self):
        """Tests adapters and converters."""
        logger.info("Verifying transformer functions.")
        for i, (engine, (opts, kwargs)) in enumerate(self._connections.items()):
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases
            with self.subTest(engine) if hasattr(self, "subTest") else contextlib.nested():  # Py3/2
                if i: logger.info("-" * 60)
                dblite.init(opts, **kwargs)
                dblite.register_adapter(json.dumps, dict)
                dblite.register_converter(json.loads, "JSON")
                self.verify_transformers(dblite, engine)
                self.verify_transformers(dblite.init(), engine)
                with dblite.transaction() as tx: self.verify_transformers(tx, engine)
                dblite.close()


    def verify_transformers(self, obj, engine):
        """Verifies adapters and converters."""
        logger.info("Verifying adapters and converters for %s %s.", engine, label(obj))
        for table, cols in self.TABLES.items():
            obj.executescript("DROP TABLE IF EXISTS %s" % table)
            if "postgres" == engine:
                cols = [dict(c, type="TIMESTAMPTZ") if "TIMESTAMP" in c["type"] else c
                        for c in cols]
            obj.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))

            for data in self.DATAS[table]:
                obj.insert(table, data)
                row, data = obj.fetchone(table, id=data["id"]), dict(data)
                row_dt, data_dt = row.pop("dt"), data.pop("dt")  # Zone handling differs in engines
                self.assertEqual(row, data, "Unexpected value from %s.select()." % label(obj))
                self.assertEqual(type(row_dt), type(data_dt),
                                 "Unexpected type for timestamp from %s.select()." % label(obj))

            obj.executescript("DROP TABLE %s" % table)


def label(obj):
    """Returns readable name for logging, for `dblite` module or class instances."""
    if isinstance(obj, dblite.api.Queryable):
        return "%s.%s" % (obj.__class__.__module__, obj.__class__.__name__)
    return obj.__name__



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_transformers] %(message)s"
    )
    unittest.main()
