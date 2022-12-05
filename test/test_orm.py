#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test a simple ORM system.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     25.11.2022
@modified    05.12.2022
------------------------------------------------------------------------------
"""
import collections
import contextlib
import datetime
import logging
import unittest

import dblite

logger = logging.getLogger()


FIELDS = ("id", "name", "type", "description")


TupleDevice = collections.namedtuple("devices", FIELDS)


class SlotDevice(object):
    __slots__ = FIELDS
    def __init__(self, id=None, name=None, type=None, description=None):
        self.id   = id
        self.name = name
        self.type = type
        self.description = description
SlotDevice.__name__ = "devices"


class SlotDict(dict):
    """Simple attrdict for slot-based attributes in subclasses."""

    def __init__(self, *args, **kwargs):
        if len(args) > 1: raise TypeError("%s expected at most 1 argument, got %s" %
                                          (type(self).__name__, len(args)))
        super(SlotDict, self).__init__()
        data = kwargs if not args else dict(args[0], **kwargs)
        x = next((k for k in data if k not in self.__slots__), None)
        if x: raise TypeError("%r is an invalid argument for %s()" % (x, type(self).__name__))
        self.update(data)

    def __getattr__(self, name):
        if name in self.__slots__:
            if name not in self:
                raise AttributeError("%r has no attribute %r" % (type(self).__name__, name))
            return self[name]
        return self.__getattribute__(name)

    def __setattr__(self, key, value):
        if key in self.__slots__ or key in type(self).__dict__:
            self[key] = value
            return
        raise AttributeError("type object %r has no attribute %r" %(type(self).__name__, key))

    def __delattr__(self, name):
        if name in self.__slots__:
            del self[name]
            return
        raise AttributeError("type object %r has no attribute %r" %(type(self).__name__, name))

    def __setitem__(self, key, value):
        if key not in self.__slots__:
            raise AttributeError("type object %r has no attribute %r" % (type(self).__name__, key))
        dict.__setitem__(self, key, value)

class SlotDictDevice(SlotDict):
    __slots__ = FIELDS
SlotDictDevice.__name__ = "devices"


class ClassDevice(object):
    def __init__(self, id=None, name=None, type=None, description=None):
        self._id   = id
        self._name = name
        self._type = type
        self._description = description

    def get_id(self): return self._id
    def set_id(self, id): self._id = id
    id = property(get_id, set_id)

    def get_name(self): return self._name
    def set_name(self, name): self._name = name
    name = property(get_name, set_name)

    def get_type(self): return self._type
    def set_type(self, type): self._type = type
    type = property(get_type, set_type)

    def get_description(self): return self._description
    def set_description(self, description): self._description = description
    description = property(get_description, set_description)
ClassDevice.__name__ = "devices"


Booking = collections.namedtuple("_", ("group", "table", "when", "patron"))
Booking.__name__ = "restaurant bookings"


class TestORM(unittest.TestCase):
    """Tests an ORM-like interface."""

    ## Engine parameters as {engine: (opts, kwargs)}
    ENGINES = {
        "sqlite":   (":memory:", {}),
        "postgres": ({}, {"maxconn": 2}),
    }

    ## Database schema to use, `%(pktype)s` will be replaced with engine-specific type
    SCHEMA = [
        "CREATE TABLE devices (id %(pktype)s PRIMARY KEY, name TEXT, type TEXT, description TEXT)",
        'CREATE TABLE "restaurant bookings" '
                                '("group" TEXT, "table" TEXT, "when" TIMESTAMP, "PATRON" BOOLEAN)',
    ]

    ## Statements to run on schema cleanup
    SCHEMA_CLEANUP = [
        "DROP TABLE IF EXISTS devices",
        'DROP TABLE IF EXISTS "restaurant bookings"',
    ]

    ## Primary key types per engine, as {engine: typename}
    PKTYPES = collections.defaultdict(lambda: "INTEGER", postgres="BIGSERIAL")

    def __init__(self, *args, **kwargs):
        super(TestORM, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass
        self._connections = collections.OrderedDict()  # {engine: (opts, kwargs)}


    def setUp(self):
        """Creates engine connection options."""
        super(TestORM, self).setUp()
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


    def test_orm(self):
        """Tests support for object-relational mapping."""
        logger.info("Verifying Object-Relational Mapping support.")

        for i, (engine, (opts, kwargs)) in enumerate(self._connections.items()):
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases
            with self.subTest(engine) if hasattr(self, "subTest") else contextlib.nested():  # Py3/2
                if i: logger.info("-" * 60)
                logger.info("Verifying Object-Relational Mapping support for %s.", engine)
                sql = ";".join(self.SCHEMA_CLEANUP + self.SCHEMA)
                dblite.init(opts, **kwargs).executescript(sql % dict(pktype=self.PKTYPES[engine]))
                self.verify_class(engine)
                self.verify_slots(engine)
                self.verify_slotsdict(engine)
                self.verify_namedtuple(engine)
                self.verify_quote(engine)
                dblite.executescript(";".join(self.SCHEMA_CLEANUP))
                dblite.close()


    def verify_class(self, engine):
        """Tests support for simple data classes."""
        logger.info("Verifying support for data classes for %s.", engine)
        cls, vals = ClassDevice, vars
        self.verify_object_interface("data classes", cls, cls, vals)


    def verify_slots(self, engine):
        """Tests support for classes with __slots__."""
        logger.info("Verifying support for slot classes for %s.", engine)
        cls, vals = SlotDevice, lambda x: [getattr(x, k) for k in FIELDS]
        self.verify_object_interface("slot classes", cls, cls, vals)


    def verify_slotsdict(self, engine):
        """Tests support for dict classes with __slots__."""
        logger.info("Verifying support for slot classes derived from dict for %s.", engine)
        cls, vals = SlotDictDevice, lambda x: {k: v for k, v in x.items() if v is not None}
        def ctor(*a, **kw):
            return cls(**dict(zip(FIELDS, [None] * len(FIELDS)), **dict(zip(FIELDS, a), **kw)))
        self.verify_object_interface("slot classes derived from dict", cls, ctor, vals)


    def verify_namedtuple(self, engine):
        """Tests support for namedtuples."""
        logger.info("Verifying support for namedtuples for %s.", engine)
        cls, vals = TupleDevice, lambda x: x
        def ctor(*a, **kw):
            return cls(**dict(zip(FIELDS, [None] * len(FIELDS)), **dict(zip(FIELDS, a), **kw)))
        self.verify_object_interface("namedtuples", cls, ctor, vals)


    def verify_object_interface(self, label, cls, ctor, extract):
        """Generic function to test support for specific type of data class."""
        Device = cls
        vals = lambda x: x if x is None else extract(x)

        logger.debug("Verifying inserting %s.", label)
        device = ctor(name="lidar", type="lidar")
        device_id = dblite.insert(Device, device)
        self.assertIsInstance(device_id, int, "Unexpected value from dblite.insert().")
        device = ctor(id=device_id, name=device.name, type=device.type)

        logger.debug("Verifying querying %s.", label)
        x = dblite.fetchone(Device, name=device.name)
        self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchone().")
        self.assertEqual(vals(x), vals(device), "Unexpected value from dblite.fetchone().")
        for x in dblite.select(Device):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.select().")
            self.assertEqual(vals(x), vals(device), "Unexpected value from dblite.select().")

        logger.debug("Verifying updating %s.", label)
        device = ctor(id=device.id, name="front lidar", type=device.type,
                      description="16-beam solid state")
        dblite.update(Device, device, id=device_id)

        logger.debug("Verifying querying %s.", label)
        for x in dblite.fetchall(Device, where={Device.type: device.type}):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")
            self.assertEqual(vals(x), vals(device), "Unexpected value from dblite.fetchall().")

        device2 = ctor(name="radar", type="radar", description="UMRR-96")
        device2_id = dblite.insert(Device, device2)
        device2 = ctor(device2_id, device2.name, device2.type, device2.description)

        expected = [device2, device]
        for i, x in enumerate(dblite.fetchall(Device, order={Device.name: True})):
            self.assertEqual(vals(x), vals(expected[i]), "Unexpected value from dblite.fetchall().")

        for where, expected in [
            (device2, device2),
            ({Device.name: device.name, Device.type: ("!=", device2.type),
              Device.description: ("IN", [device.description, device2.description])}, device),
            ([(Device.name, device.name), (Device.name, "IN", [device.name, device2.name]),
              (Device.name, ("!=", None))], device),
            ([(Device.name, device2.name), (Device.type, "nosuch")], None),
        ]:
            received = dblite.fetchone(Device, where=where)
            self.assertEqual(vals(received), vals(expected),
                             "Unexpected value from dblite.fetchone().")

        logger.debug("Verifying deleting %s.", label)
        dblite.delete(Device, device)
        x = dblite.fetchone(Device)
        self.assertEqual(vals(x), vals(device2), "Unexpected value from dblite.fetchone().")
        dblite.delete(Device)
        self.assertFalse(dblite.fetchone(Device), "Unexpected value from dblite.fetchone().")


    def verify_quote(self, engine):
        """Tests auto-quoting class and attribute names"""
        logger.info("Verifying auto-quoting class and attribute names for %r.", engine)

        val1 = Booking("Charity",  "Table 16", datetime.datetime(2022, 12, 30, 18, 30), False)
        val2 = Booking("MotoClub", "Table 17", datetime.datetime(2022, 12, 30, 19),     True)
        val3 = Booking("Acme INC", "Table 18", datetime.datetime(2022, 12, 30, 19),     True)
        DATAS = [val1, val2, val3]
        dblite.insert(Booking, val1)
        expected = (val1.group, val1.table, None, None)
        for entry in dblite.fetchall(Booking, (Booking.group, Booking.table), order=Booking.group):
            received = (entry.group, entry.table, entry.when, entry.patron)
            self.assertEqual(received, expected, "Unexpected value from dblite.fetchall().")

        dblite.insert(Booking, [(getattr(Booking, k), getattr(val2, k)) for k in Booking._fields])
        dblite.insert(Booking, { getattr(Booking, k): getattr(val3, k)  for k in Booking._fields})
        self.assertEqual(dblite.fetchall(Booking, order=Booking.table), DATAS,
                         "Unexpected value from dblite.fetchall().")
        for i, booking in enumerate(list(DATAS)):
            updated = Booking(booking.group, booking.table, datetime.datetime.now(), booking.patron)
            values = [(Booking.when, updated.when)] if i else {Booking.when:   updated.when}
            where  = {Booking.table: booking.table} if i else [(Booking.table, booking.table)]
            dblite.update(Booking, values, where)
            self.assertEqual(dblite.fetchone(Booking, where=where), updated,
                             "Unexpected value from dblite.fetchone().")
            DATAS[i] = updated
        group = (Booking.group, Booking.table, Booking.when, Booking.patron)
        for i, entry in enumerate(dblite.select(Booking, group=group, order=Booking.table)):
            self.assertEqual(entry, DATAS[i], "Unexpected value from dblite.select().")



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_orm] %(message)s"
    )
    unittest.main()
