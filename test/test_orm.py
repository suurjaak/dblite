#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test a simple ORM system.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     25.11.2022
@modified    28.11.2022
------------------------------------------------------------------------------
"""
import collections
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


Booking = collections.namedtuple("_", ("group", "table", "date"))
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
        'CREATE TABLE "restaurant bookings" ("group" TEXT, "table" TEXT, "date" TIMESTAMP)',
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
            dblite.init(opts, **kwargs)
        except psycopg2.Error as e:
            logger.warning("Skip testing postgres, connection failed with:\n%s", e)
        else:
            self._connections["postgres"] = (opts, kwargs)
        dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def test_orm(self):
        """Tests support for object-relational mapping."""
        logger.info("Verifying Object-Relational Mapping support.")

        for engine, (opts, kwargs) in self._connections.items():
            logger.info("Verifying Object-Relational Mapping support for %s.", engine)
            schema = ";".join(self.SCHEMA) % dict(pktype=self.PKTYPES[engine])
            dblite.init(opts, **kwargs).executescript(schema)
            self.verify_class(engine)
            self.verify_slots(engine)
            self.verify_slotsdict(engine)
            self.verify_namedtuple(engine)
            self.verify_quote(engine)
            dblite.executescript(";".join(self.SCHEMA_CLEANUP))
            dblite.close()
            dblite.api.Engines.DATABASES.clear()  # Clear cache of default databases


    def verify_class(self, engine):
        """Tests support for simple data classes."""
        logger.info("Verifying support for data classes for %r.", engine)
        Device = ClassDevice

        logger.debug("Verifying inserting objects.")
        device = Device()
        device.name = "lidar"
        device.id = dblite.insert(Device, device)
        self.assertIsInstance(device.id, int, "Unexpected value from dblite.insert().")

        logger.debug("Verifying querying objects.")
        device = dblite.fetchone(Device, name=device.name)
        self.assertIsInstance(device, Device, "Unexpected value from dblite.fetchone().")
        for x in dblite.select(Device):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")

        logger.debug("Verifying updating objects.")
        device.name = "front lidar"
        device.type = "lidar"
        device.description = "16-beam solid state"
        dblite.update(Device, device, id=device.id)

        logger.debug("Verifying querying objects.")
        devices = dblite.fetchall(Device, where={Device.type: device.type})
        for x in devices:
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")
        self.assertEqual([vars(x) for x in devices], [vars(device)],
                         "Unexpected value from dblite.fetchall().")

        devices = dblite.fetchall(Device, order=Device.name)

        logger.debug("Verifying deleting objects.")
        dblite.delete(Device, device)
        self.assertFalse(dblite.fetchone(Device), "Unexpected value from dblite.fetchone().")


    def verify_slots(self, engine):
        """Tests support for classes with __slots__."""
        logger.info("Verifying support for slot classes for %r.", engine)
        Device = SlotDevice

        logger.debug("Verifying inserting objects.")
        device = Device(name="lidar")
        device.id = dblite.insert(Device, device)
        self.assertIsInstance(device.id, int, "Unexpected value from dblite.insert().")

        logger.debug("Verifying querying objects.")
        device = dblite.fetchone(Device, name=device.name)
        self.assertIsInstance(device, Device, "Unexpected value from dblite.fetchone().")
        for x in dblite.select(Device):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")

        logger.debug("Verifying updating objects.")
        device.name = "front lidar"
        device.type = "lidar"
        device.description = "16-beam solid state"
        dblite.update(Device, device, id=device.id)

        logger.debug("Verifying querying objects.")
        devices = dblite.fetchall(Device, where={Device.type: device.type})
        for x in devices:
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")
        self.assertEqual([[getattr(x, k) for  k in FIELDS] for x in devices],
                         [[getattr(device, k) for  k in FIELDS]],
                         "Unexpected value from dblite.fetchall().")

        devices = dblite.fetchall(Device, order=Device.name)

        logger.debug("Verifying deleting objects.")
        dblite.delete(Device, device)
        self.assertFalse(dblite.fetchone(Device), "Unexpected value from dblite.fetchone().")


    def verify_slotsdict(self, engine):
        """Tests support for dict classes with __slots__."""
        logger.info("Verifying support for slot classes derived from dict for %r.", engine)
        Device = SlotDictDevice

        logger.debug("Verifying inserting objects.")
        device = Device(name="lidar")
        device.id = dblite.insert(Device, device)
        self.assertIsInstance(device.id, int, "Unexpected value from dblite.insert().")

        logger.debug("Verifying querying objects.")
        device = dblite.fetchone(Device, name=device.name)
        self.assertIsInstance(device, Device, "Unexpected value from dblite.fetchone().")
        for x in dblite.select(Device):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")

        logger.debug("Verifying updating objects.")
        device.name = "front lidar"
        device.update(type="lidar", description="16-beam solid state")
        dblite.update(Device, device, {Device.id: device.id})

        logger.debug("Verifying querying objects.")
        devices = dblite.fetchall(Device, where={Device.type: device.type})
        for x in devices:
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")
        self.assertEqual(devices, [device], "Unexpected value from dblite.fetchall().")

        devices = dblite.fetchall(Device, order=Device.name)

        logger.debug("Verifying deleting objects.")
        dblite.delete(Device, device)
        self.assertFalse(dblite.fetchone(Device), "Unexpected value from dblite.fetchone().")


    def verify_namedtuple(self, engine):
        """Tests support for namedtuples."""
        logger.info("Verifying support for namedtuples for %r.", engine)
        Device = TupleDevice

        logger.debug("Verifying inserting namedtuples.")
        device = Device(id=None, name="lidar", type=None, description=None)
        device_id = dblite.insert(Device, device)
        self.assertIsInstance(device_id, int, "Unexpected value from dblite.insert().")

        logger.debug("Verifying querying namedtuples.")
        device = dblite.fetchone(Device, name=device.name)
        self.assertIsInstance(device, Device, "Unexpected value from dblite.fetchone().")
        for x in dblite.select(Device):
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")

        logger.debug("Verifying updating namedtuples.")
        device = Device(id=device_id, name="front lidar", type="lidar", description="16-beam solid state")
        dblite.update(Device, device, id=device_id)

        logger.debug("Verifying querying namedtuples.")
        devices = dblite.fetchall(Device, where={Device.type: device.type})
        for x in devices:
            self.assertIsInstance(x, Device, "Unexpected value from dblite.fetchall().")
        self.assertEqual([x._asdict() for x in devices], [device._asdict()],
                         "Unexpected value from dblite.fetchall().")

        devices = dblite.fetchall(Device, order=Device.name)

        logger.debug("Verifying deleting namedtuples.")
        dblite.delete(Device, device)
        self.assertFalse(dblite.fetchone(Device), "Unexpected value from dblite.fetchone().")


    def verify_quote(self, engine):
        """Tests auto-quoting class and attribute names"""
        logger.info("Verifying auto-quoting class and attribute names for %r.", engine)

        booking = Booking("Charity", "Table 16", datetime.datetime(2022, 12, 30, 18, 30))
        dblite.insert(Booking, booking)
        expected = (booking.group, booking.table, None)
        for entry in dblite.fetchall(Booking, (Booking.group, Booking.table), order=Booking.group):
            received = (entry.group, entry.table, entry.date)
            self.assertEqual(received, expected, "Unexpected value from dblite.fetchall().")



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_orm] %(message)s"
    )
    unittest.main()
