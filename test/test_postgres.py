#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test Postgres-specific aspects.

Running needs sufficient variables in environment like `PGUSER`.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     22.11.2022
@modified    22.11.2022
------------------------------------------------------------------------------
"""
import collections
import logging
import os
import unittest

import dblite
import dblite.engines.postgres

logger = logging.getLogger()


class TestPostgres(unittest.TestCase):
    """Tests Postgres-specific aspects."""

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
        super(TestPostgres, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._env = collections.defaultdict(str)  # Postgres environment variables, as {"user": ..}


    def setUp(self):
        """Populates Postgres variables from environment."""
        super(TestPostgres, self).setUp()
        for k, v in os.environ.items():
            if not k.startswith("PG"): continue  # for k, v
            self._env[k[2:].lower()] = v

    def test_postgres(self):
        """Tests Postgres-specific aspects."""
        logger.info("Verifying Postgres-specific aspects.")

        try: import psycopg2
        except ImportError:
            logger.warning("Skip testing postgres, psycopg2 not available.")
            return
        try:
            dblite.init({})
        except psycopg2.Error as e:
            logger.warning("Skip testing postgres, connection failed with:\n%s", e)
            return

        logger.info("Verifying Postgres connection options.")
        uri = "postgresql://"
        if self._env.get("user"):     uri += self._env["user"]
        if self._env.get("password"): uri += ":%s" % self._env["password"]
        if self._env.get("user") or self._env.get("password"): uri += "@"
        if self._env.get("host"):     uri += self._env["host"]
        if self._env.get("port"):     uri += ":%s" % self._env["port"]
        if self._env.get("dbname"):   uri += "/%s" % self._env["dbname"]
        for opts in [uri, " ".join("%s=%s" % x for x in self._env.items()), self._env]:
            self.assertIsInstance(dblite.init(opts), dblite.engines.postgres.Database,
                                  "Unexpected value from dblite.init().")
            dblite.close()
        dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases

        dblite.init(self._env, maxconn=5)
        logger.info("Verifying postgres.Transaction(schema).")
        with dblite.transaction(schema="information_schema") as tx:
            tx.fetchone("columns")

        logger.info("Verifying postgres.Transaction(lazy).")
        with dblite.transaction(lazy=True) as tx:
            for _ in tx.select("information_schema.columns"):
                break  # for _

        logger.info("Verifying concurrent transactions.")
        with dblite.transaction() as tx:
            for table, cols in self.TABLES.items():
                tx.executescript("DROP TABLE IF EXISTS %s" % table)
                tx.executescript("CREATE TABLE %s (%s)" %
                                  (table, ", ".join("%(name)s %(type)s" % c for c in cols)))

        with dblite.transaction() as tx1, dblite.transaction() as tx2, \
             dblite.transaction() as tx3, dblite.transaction() as tx4:
            logger.debug("Verifying isolated state for concurrent transactions.")
            for table, datas in self.DATAS.items():
                for i, tx in enumerate([tx1, tx2, tx3, tx4]):
                    tx.insert(table, datas[i])

                for i, tx in enumerate([tx1, tx2, tx3, tx4]):
                    rows = tx.fetchall(table)
                    self.assertEqual(set(x["id"] for x in rows), set([datas[i]["id"]]),
                                     "Unexpected value from tx.select().")

        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table, order="id")
                self.assertEqual(rows, datas, "Unexpected value from tx.select().")
                tx.executescript("DROP TABLE %s" % table)



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_postgres] %(message)s"
    )
    unittest.main()
