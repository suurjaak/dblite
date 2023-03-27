#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test SQLite-specific aspects.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     22.11.2022
@modified    27.03.2023
------------------------------------------------------------------------------
"""
import datetime
import logging
import os
import sqlite3
import tempfile
import unittest

import dblite
import dblite.engines.sqlite

logger = logging.getLogger()


class TestSQLite(unittest.TestCase):
    """Tests SQLite-specific aspects."""

    ## Table columns as {table name: [{"name", "type"}]}
    TABLES = {
        "test": [{"name": "id",  "type": "INTEGER PRIMARY KEY"},
                 {"name": "dt",  "type": "TIMESTAMP"},
                 {"name": "val", "type": "TEXT"}],
    }

    ## Table test data, as {table name: [{row}]}
    DATAS = {
        "test": [
            {"id": 1, "dt": None, "val": "val1"},
            {"id": 2, "dt": None, "val": "val2"},
            {"id": 3, "dt": None, "val": "val3"},
            {"id": 4, "dt": None, "val": "val4"},
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TestSQLite, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._paths = []  # [path to SQLite database, ]


    def setUp(self):
        """Populates temporary file paths."""
        super(TestSQLite, self).setUp()
        try: import pathlib  # Py3
        except ImportError: pathlib = None  # Py2
        for i in range(2):
            with tempfile.NamedTemporaryFile(suffix=".sqlite") as f:
                logger.debug("Making temporary SQLite database %s.", f.name)
                self._paths.append(pathlib.Path(f.name) if i and pathlib else f.name)


    def tearDown(self):
        """Deletes temoorary files."""
        for path in self._paths:
            try: os.remove(path)
            except Exception: pass
        super(TestSQLite, self).tearDown()


    def test_sqlite(self):
        """Tests SQLite-specific aspects."""
        logger.info("Verifying SQLite-specific aspects.")

        logger.info("Verifying concurrent working with multiple files.")
        dbs = [dblite.init(p) for p in self._paths]
        for table, cols in self.TABLES.items():
            for db in dbs:
                db.executescript("CREATE TABLE %s (%s)" %
                                 (table, ", ".join("%(name)s %(type)s" % c for c in cols)))
        logger.debug("Verifying data isolation between files.")
        for table, datas in self.DATAS.items():
            for i, data in enumerate(datas):
                dbs[i % len(dbs)].insert(table, data)
            for i, db in enumerate(dbs):
                expected_ids = [x["id"] for x in datas[i % len(dbs)::len(dbs)]]
                rows = db.fetchall(table)
                self.assertEqual(set(x["id"] for x in rows), set(expected_ids),
                                 "Unexpected value from db.select().")
                db.delete(table)
                db.close()

        logger.info("Verifying detect_types parameter.")
        dbs = [dblite.init(p, detect_types=sqlite3.PARSE_DECLTYPES if i else 0)
               for i, p in enumerate(self._paths)]
        for table, datas in self.DATAS.items():
            for i, data in enumerate(datas):
                data["dt"] = datetime.datetime.now()
                dbs[i % len(dbs)].insert(table, data)
            for i, db in enumerate(dbs):
                checker, expected = (all, True) if i else (any, False)
                rows = db.fetchall(table)
                self.assertEqual(checker(isinstance(x["dt"], datetime.datetime) for x in rows),
                                 expected, "Unexpected value from db.select(): %s" % rows)
                db.delete(table)
                db.close()

        logger.info("Verifying concurrent working with multiple connections to single file.")
        dbs = [dblite.init(p) for p in self._paths[:1] * 2]
        for table, datas in self.DATAS.items():
            logger.info("Verifying writing data intermittently on different connections.")

            for i, data in enumerate(datas):  # Insert on one connection should appear in the other
                dbs[i % len(dbs)].insert(table, data)
                row = dbs[not i % len(dbs)].fetchone(table, where=data)
                self.assertEqual(row, data, "Unexpected value from db.select().")
            for db in dbs:
                rows = db.fetchall(table)
                self.assertEqual(rows, datas, "Unexpected value from db.select().")

        for db in dbs:
            db.close()



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_sqlite] %(message)s"
    )
    unittest.main()
