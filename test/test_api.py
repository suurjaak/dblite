#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test database general API in available engines.

Running Postgres test needs sufficient variables in environment like `PGUSER`.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     20.11.2022
@modified    06.12.2022
------------------------------------------------------------------------------
"""
import collections
import contextlib
import copy
import logging
import os
import tempfile
import threading
import time
import unittest

import dblite

logger = logging.getLogger()


class TestAPI(unittest.TestCase):
    """Tests dblite API."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   ("", {}),
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
        super(TestAPI, self).tearDown()


    def test_api(self):
        """Tests dblite API."""
        for i, (engine, (opts, kwargs)) in enumerate(self._connections.items()):
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases
            with self.subTest(engine) if hasattr(self, "subTest") else contextlib.nested():  # Py3/2
                if i: logger.info("-" * 60)
                logger.info("Verifying dblite API for %s.", engine)
                self.verify_general_api(opts, kwargs, engine)
                self.verify_query_api(dblite)
                dblite.close()
                with dblite.init() as db:
                    self.verify_query_api(db)
                with dblite.transaction() as tx:
                    self.verify_query_api(tx)
                self.verify_query_args(dblite)
                self.verify_transactions()
                self.verify_exclusive_transactions()


    def verify_general_api(self, opts, kwargs, engine):
        """Verifies general module-level and Database-level functions."""
        logger.info("Verifying dblite general module and Database functions.")

        logger.debug("Verifying dblite.init(engine=%r).", engine)
        db = dblite.init(opts, engine=engine, **kwargs)
        self.assertIsInstance(db, dblite.api.Database, "Unexpected value from dblite.init().")
        db2 = dblite.init()
        self.assertIs(db2, db, "Unexpected value from dblite.init().")

        logger.debug("Verifying dblite.transaction().")
        tx = db.transaction()
        self.assertIsInstance(tx, dblite.api.Transaction,
                              "Unexpected value from dblite.transaction().")
        tx.close()

        logger.info("Verifying Database property decorators.")
        self.assertFalse(db.closed, "Unexpected value from %s.closed." % label(db))
        self.assertIsNotNone(db.cursor, "Unexpected value from %s.cursor." % label(db))

        logger.info("Verifying dblite.close().")
        self.assertFalse(db.closed, "Unexpected value from %s.closed." % label(db))
        dblite.close()
        self.assertTrue(db.closed, "Unexpected value from %s.closed." % label(db))
        self.assertIsNone(db.cursor, "Unexpected value from %s.cursor." % label(db))
        logger.debug("Verifying dblite.close() denying further queries.")
        with self.assertRaises(Exception,
                               msg="Unexpected success for fetch after closing database."):
            db.fetchone(next(iter(self.TABLES)))


    def verify_query_api(self, obj):
        """Verifies query functions."""
        logger.info("Verifying %s query functions.", label(obj))
        DATAS = copy.deepcopy(self.DATAS)

        for table, cols in self.TABLES.items():
            obj.executescript("DROP TABLE IF EXISTS %s" % table)
            obj.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))
            logger.debug("Verifying %s.insert(%r).", label(obj), table)
            for data in DATAS[table]:
                myid = obj.insert(table, data)
                self.assertEqual(myid, data["id"], "Unexpected value from %s.insert()." % obj)
            logger.debug("Verifying %s.fetchone(%r).", label(obj), table)
            for data in DATAS[table]:
                row = obj.fetchone(table, id=data["id"])
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
            logger.debug("Verifying %s.fetchall(%r).", label(obj), table)
            rows = obj.fetchall(table)
            self.assertEqual(rows, DATAS[table], "Unexpected value from %s.fetchall()." % obj)

            logger.debug("Verifying %s.update(%r).", label(obj), table)
            for data in DATAS[table]:
                data.update(val=data["val"] * 3)  # Update DATAS
                affected = obj.update(table, data, id=data["id"])
                self.assertEqual(affected, 1, "Unexpected value from %s.update()." % obj)
                row = obj.fetchone(table, id=data["id"])
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
        obj.close()
        if isinstance(obj, dblite.api.Database):
            logger.debug("Closing and reopening %s.", label(obj))
            obj.open()
        elif isinstance(obj, dblite.api.Transaction):
            logger.debug("Closing and remaking %s.", label(obj))
            obj = obj.database.transaction()  # Create new transaction for verifying persistence
        else:
            logger.debug("Closing and reopening database.")
            obj.init()

        logger.info("Verifying %s data persistence.", label(obj))
        for table, cols in self.TABLES.items():
            for data in DATAS[table]:
                row = obj.fetchone(table, id=data["id"])
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % obj)
            rows = obj.fetchall(table)
            self.assertEqual(rows, DATAS[table], "Unexpected value from %s.fetchall()." % obj)

            logger.debug("Verifying %s.delete(%r).", label(obj), table)
            for data in DATAS[table][::2]:
                affected = obj.delete(table, id=data["id"])
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
            obj.close()  # Close the re-created transaction


    def verify_query_args(self, obj):
        """Verifies various ways of providing query parameters."""
        logger.info("Verifying %s query parameters.", label(obj))
        class Column(object):
            """Simple stringable class."""
            def __init__(self, name): self.name = name
            def __repr__(self): return "Column(%r)" % self.name
            def __str__(self):  return self.name

        for table, cols in self.TABLES.items():
            obj.executescript("DROP TABLE IF EXISTS %s" % table)
            obj.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))

        logger.info("Verifying INSERT arguments.")
        for table, cols in self.TABLES.items():
            for i, data in enumerate(self.DATAS[table]):
                if i < 2: obj.insert(table, data if i else list(data.items()))
                else:     obj.insert(table, **data)
                row = obj.fetchone(table, where=data if i else list(data.items())) if i < 2 else \
                      obj.fetchone(table, **data)
                self.assertEqual(row, data, "Unexpected value from %s.fetchone()." % label(obj))

        logger.info("Verifying SELECT columns.")
        for table, cols in self.TABLES.items():
            for col in (", ".join(sorted(c["name"] for c in cols)),
                        [Column(c["name"]) for c in cols],
                        "*",
                        {c["name"]: 0 for c in cols},
                        [c["name"] for c in cols], [c["name"] for c in cols][::2]):
                row = obj.fetchone(table, col)
                received = set(row) if isinstance(col, (dict, list)) else ", ".join(sorted(row))
                expected = set(map(str, col)) if isinstance(col, (dict, list)) else \
                           ", ".join(sorted(c["name"] for c in cols)) if col == "*" else col
                self.assertEqual(received, expected,
                                 "Unexpected value from %s.select(cols)." % label(obj))

        logger.info("Verifying SELECT WHERE.")
        for table, cols in self.TABLES.items():
            example = self.DATAS[table][0]
            WHERES = [
                example,
                list(example.items()),
                {Column(k): v for k, v in example.items()},
                [("id", "IN", [example["id"]])],
                "id = %s" % example["id"],
                [("val", ("!=", None)), ("EXPR", ("id >= ? OR id <= ?", [example["id"]]*2))],
                {"id": example["id"], "val": ("NOT IN", [])},
                [("id", example["id"]), ("val", "LIKE", "%%%s%%" % example["val"])],
                [("id", "=", example["id"]), ("LENGTH(val)", len(example["val"]))],
            ]
            for where in WHERES:
                logger.debug("Verifying WHERE %r", (where, ))
                self.assertEqual(obj.fetchone(table, where=where), example,
                                 "Unexpected value from %s.select(where=%s)." % (label(obj), where))

        logger.info("Verifying SELECT LIMIT.")
        for table, cols in self.TABLES.items():
            for limit in (0, 2, (2, 1), (-1, 1), (None, 1), (None, None), (-1, None)):
                logger.debug("Verifying LIMIT %r", (limit, ))
                LIMIT = next(v for v in [limit if isinstance(limit, int) else limit[0]])
                LIMIT = len(self.DATAS[table]) if LIMIT in (-1, None) else LIMIT
                OFFSET = (0 if isinstance(limit, int) or limit[1] is None or limit[1] < 0 else limit[1])
                expected_count = min(LIMIT, len(self.DATAS[table]) - OFFSET)
                expected_ids   = [x["id"] for i, x in enumerate(self.DATAS[table])
                                  if i >= OFFSET and (i - OFFSET) < LIMIT]
                rows = obj.fetchall(table, order="id", limit=limit)
                self.assertEqual(len(rows), expected_count,
                                 "Unexpected value from %s.select(limit=%s)." % (label(obj), limit))
                self.assertEqual(set(x["id"] for x in rows), set(expected_ids),
                                 "Unexpected value from %s.select(limit=%s)." % (label(obj), limit))

        DATAS = copy.deepcopy(self.DATAS)
        logger.info("Verifying UPDATE arguments.")
        for table in DATAS:
            for i, data in enumerate(DATAS[table]):
                # Set alternating values for later ORDER BY verifying
                data["val"] = chr(ord("Z") - (data["id"] - 1) % 2) # Updates DATAS
                if i < 2: obj.update(table, data if i else list(data.items()), {"id": data["id"]})
                else:     obj.update(table, data, id=data["id"])
                row = obj.fetchone(table, where=data)
                self.assertEqual(row, data, "Unexpected value from %s.select()." % label(obj))

        logger.info("Verifying ORDER BY arguments.")
        for table in DATAS:
            ORDERS = [  # [(argument value, [(col, direction), ])]
                (Column("id"),                    [("id",  True), ]),
                ("id ASC",                        [("id",  True), ]),
                ("id DESC",                       [("id",  False), ]),
                ([Column("id"), True],            [("id",  True),  ]),
                ("val, id DESC",                  [("val", True),  ("id", False)]),
                ([Column("val"), "id DESC"],      [("val", True),  ("id", False)]),
                (["val", (Column("id"), "DESC")], [("val", True),  ("id", False)]),
                (["val DESC", ("id", True)],      [("val", False), ("id", True)]),
                (collections.OrderedDict([("val", False), ("id", True)]), 
                                                  [("val", False), ("id", True)]),
            ]
            for order, sorts in ORDERS:
                logger.debug("Verifying ORDER BY %r", order)
                reverse = ("val" == sorts[0][0]) and not sorts[0][1]
                expected_order = sorted(DATAS[table],
                    key=lambda x: [-x[k] if "id" == k and asc == reverse else x[k]
                                   for k, asc in sorts], reverse=reverse
                )
                self.assertEqual(obj.fetchall(table, order=order), expected_order,
                                 "Unexpected value from %s.select(order=%r)." % (label(obj), order))

        logger.info("Verifying GROUP BY arguments.")
        for table in DATAS:
            expected_ids = all_ids = [x["id"] for x in DATAS[table]]
            for group in ("id", Column("id"), "id, val", ["id"], [Column("id"), "val"], 1):
                logger.debug("Verifying GROUP BY %r", group)
                rows = obj.fetchall(table, group=group)
                self.assertEqual(set(x["id"] for x in rows), set(expected_ids),
                                 "Unexpected value from %s.select(group=%r)." % (label(obj), group))
            expected_ids = [max(v for v in all_ids if v % 2 == m) for m in (1, 0)]
            rows = obj.fetchall(table, "MAX(id) AS id", group="id % 2")
            self.assertEqual(set(x["id"] for x in rows), set(expected_ids),
                             "Unexpected value from %s.select(group=%r)." % (label(obj), "id % 2"))

        for table in DATAS:
            obj.executescript("DROP TABLE %s" % table)


    def verify_transactions(self):
        """Verifies transactions."""
        logger.info("Verifying transactions.")

        for table, cols in self.TABLES.items():
            dblite.executescript("DROP TABLE IF EXISTS %s" % table)
            dblite.executescript("CREATE TABLE %s (%s)" %
                              (table, ", ".join("%(name)s %(type)s" % c for c in cols)))

        logger.info("Verifying commit and rollback.")
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                tx.insert(table, datas[0])
                row = tx.fetchone(table, id=datas[0]["id"])
                self.assertEqual(row, datas[0], "Unexpected value from %s.select()." % label(tx))
            logger.debug("Verifying mid-transaction commit.")
            tx.commit()
            for table, datas in self.DATAS.items():
                tx.insert(table, datas[1])
                row = tx.fetchone(table, id=datas[1]["id"])
                self.assertEqual(row, datas[1], "Unexpected value from %s.select()." % label(tx))
            logger.debug("Verifying mid-transaction rollback.")
            tx.rollback()
            for table, datas in self.DATAS.items():
                row = tx.fetchone(table, id=datas[1]["id"])
                self.assertIsNone(row, "Unexpected value from %s.select()." % label(tx))
            for table, datas in self.DATAS.items():
                tx.insert(table, datas[1])
                row = tx.fetchone(table, id=datas[1]["id"])
                self.assertEqual(row, datas[1], "Unexpected value from %s.select()." % label(tx))

        logger.info("Verifying raising Rollback.")
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[:2], "Unexpected value from %s.select()." % label(tx))
                affected = tx.delete(table)
                self.assertEqual(affected, 2, "Unexpected value from %s.delete()." % label(tx))
            raise dblite.Rollback
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[:2], "Unexpected value from %s.select()." % label(tx))

        logger.info("Verifying Transaction(commit=False).")
        with dblite.transaction(commit=False) as tx:
            for table in self.DATAS:
                tx.delete(table)
                rows = tx.fetchall(table)
                self.assertEqual(rows, [], "Unexpected value from %s.select()." % label(tx))
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[:2], "Unexpected value from %s.select()." % label(tx))

        logger.info("Verifying Transaction.close().")
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                tx.delete(table, datas[0])
            tx.close(commit=False)
        logger.debug("Verifying Transaction(commit=False).close().")
        with dblite.transaction(commit=False) as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[:2], "Unexpected value from %s.select()." % label(tx))
                tx.delete(table, datas[0])
            tx.close()
        logger.debug("Verifying Transaction(commit=False).close(commit=True).")
        with dblite.transaction(commit=False) as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[:2], "Unexpected value from %s.select()." % label(tx))
                tx.delete(table, datas[0])
            tx.close(commit=True)
        logger.debug("Verifying Transaction(commit=True).close(commit=None).")
        with dblite.transaction() as tx:
            for table, datas in self.DATAS.items():
                rows = tx.fetchall(table)
                self.assertEqual(rows, datas[1:2], "Unexpected value from %s.select()." % label(tx))
                tx.delete(table, datas[1])
            tx.close()
        with dblite.transaction() as tx:
            for table in self.DATAS:
                rows = tx.fetchall(table)
                self.assertEqual(rows, [], "Unexpected value from %s.select()." % label(tx))
            tx.close()
        logger.debug("Verifying Transaction.close() denying further queries.")
        with self.assertRaises(Exception,
                               msg="Unexpected success for fetch after closing transaction."):
            tx.fetchone(table)

        for table in self.DATAS:
            dblite.execute("DROP TABLE %s" % table)

        logger.info("Verifying Transaction property decorators.")
        with dblite.transaction() as tx:
            self.assertFalse(tx.closed, "Unexpected value from %s.closed." % label(tx))
            self.assertIsNotNone(tx.cursor, "Unexpected value from %s.cursor." % label(tx))
            self.assertIsInstance(tx.database, dblite.Database,
                                  "Unexpected value from %s.database." % label(tx))
        self.assertTrue(tx.closed, "Unexpected value from %s.closed." % label(tx))
        self.assertIsNone(tx.cursor, "Unexpected value from %s.cursor." % label(tx))

        logger.info("Verifying Transaction.quote().")
        with dblite.transaction() as tx:
            for value, same in [("WHERE", False), ("one two", False), ("abcd", True)]:
                logger.debug("Verifying Transaction.quote(%r).", value)
                result = tx.quote(value)
                self.assertEqual(result == value, same, "Unexpected value from %s.quote(%r): %r." %
                                 (label(tx), value, result))
                if same:
                    result = tx.quote(value, force=True)
                    logger.debug("Verifying Transaction.quote(%r, force=True).", value)
                    self.assertNotEqual(result, value,
                                        "Unexpected value from %s.quote(%r, force=True): %r." %
                                        (label(tx), value, result))


    def verify_exclusive_transactions(self):
        """Verifies exclusive transactions being exclusive."""
        logger.info("Verifying exclusive transactions.")

        DELAY = 1
        def waiter(semaphore):
            """Opens transaction and sleeps for a bit."""
            with dblite.transaction(exclusive=True):
                logger.debug("Entered exclusive transaction in background thread.")
                semaphore.set()
                time.sleep(DELAY)
            logger.debug("Exited exclusive transaction in background thread.")

        semaphore = threading.Event()
        logger.debug("Firing up background thread for first transaction.")
        threading.Thread(target=waiter, args=(semaphore, )).start()
        logger.debug("Waiting for background thread to run.")
        semaphore.wait()
        t1 = time.time()
        logger.debug("Opening exclusive transaction in main thread.")
        with dblite.transaction(exclusive=True):
            logger.debug("Entered exclusive transaction in main thread.")
            t2 = time.time()
        self.assertLessEqual(t1, t2 - DELAY, "Exclusive transaction did not exclude.")


def label(obj):
    """Returns readable name for logging, for `dblite` module or class instances."""
    if isinstance(obj, dblite.api.Queryable):
        return "%s.%s" % (obj.__class__.__module__, obj.__class__.__name__)
    return obj.__name__



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_api] %(message)s"
    )
    unittest.main()
