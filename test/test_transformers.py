#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test adapters and converters.

Running Postgres test needs sufficient variables in environment lik `PGUSER`.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     22.11.2022
@modified    22.11.2022
------------------------------------------------------------------------------
"""
import collections
import datetime
import json
import logging
import os
import tempfile
import unittest

import dblite

logger = logging.getLogger()


class TestTransformers(unittest.TestCase):
    """Tests adapters and converters."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   ("", {}),
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
            {"id": 1, "dt": datetime.datetime.now(dblite.api.UTC),
             "val": {"nested": {"value": [1, 2]}}},
            {"id": 2, "dt": datetime.datetime.now(dblite.api.UTC),
             "val": {"nested": [None, False, 1.1, 2.2]}},
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TestTransformers, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._connections = collections.OrderedDict()  # {engine: (opts, kwargs)}
        self._path = None  # Path to SQLite database


    def setUp(self):
        """Creates engine connection options."""
        super(TestTransformers, self).setUp()
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
        super(TestTransformers, self).tearDown()


    def test_transformers(self):
        """Tests adapters and converters."""
        logger.info("Verifying transformer functions.")
        for engine, (opts, kwargs) in self._connections.items():
            dblite.init(opts, **kwargs)
            dblite.register_adapter(json.dumps, dict)
            dblite.register_converter(json.loads, "JSON")
            self.verify_transformers(engine)
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def verify_transformers(self, engine):
        """Verifies adapters and converters."""
        logger.info("Verifying adapters and converters for %s.", engine)
        for table, cols in self.TABLES.items():
            dblite.executescript("DROP TABLE IF EXISTS %s" % table)
            if "postgres" == engine:
                cols = [dict(c, type="TIMESTAMPTZ") if "TIMESTAMP" in c["type"] else c
                        for c in cols]
            dblite.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))

            for data in self.DATAS[table]:
                dblite.insert(table, data)
                row = dblite.fetchone(table, id=data["id"])
                row["dt"], data["dt"] = (x["dt"].replace(tzinfo=None) for x in (row, data))
                self.assertEqual(row, data, "Unexpected value from dblite.select().")


if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_transformers] %(message)s"
    )
    unittest.main()
