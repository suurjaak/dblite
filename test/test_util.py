#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test utility functions.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     19.07.2023
@modified    20.07.2023
------------------------------------------------------------------------------
"""
import collections
import datetime
import decimal
import logging
import types
import unittest

import dblite
from dblite.util import parse_datetime
import six

logger = logging.getLogger()


CNAME, CFIELDS = "dataclass", ("field1", "field2")


TupleType = collections.namedtuple(CNAME, CFIELDS)


class PlainType(object):
    def __init__(self, field1=None, field2=None):
        self.field1, self.field2 = field1, field2
PlainType.__name__ = CNAME


class SlotsType(object):
    __slots__ = CFIELDS
    def __init__(self, field1=None, field2=None):
        self.field1, self.field2 = field1, field2
SlotsType.__name__ = CNAME


class PropsType(object):
    def __init__(self, field1=None, field2=None):
        self._field1, self._field2 = field1, field2

    def get_field1(self): return self._field1
    def set_field1(self, field1): self._field1 = field1
    field1 = property(get_field1, set_field1)

    def get_field2(self): return self._field2
    def set_field2(self, field2): self._field2 = field2
    field2 = property(get_field2, set_field2)
PropsType.__name__ = CNAME


class TestUtil(unittest.TestCase):
    """Tests utility functions."""


    def __init__(self, *args, **kwargs):
        super(TestUtil, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass


    def test_factory(self):
        """Tests util.factory()."""
        FUNC = dblite.util.factory
        DATAS = [  # [([..args..], (expected, errors)), ]
            ([int,  {"no": "such"}],        ({"no": "such"},        True)),
            ([dict, {"a": 1, "b": 2}],      ({"a": 1, "b": 2},      False)),
            ([dict, [("a", 1), ("b", 2)]],  ({"a": 1, "b": 2},      False)),
            ([PlainType, (1, )],            (PlainType(1, None),    False)),
            ([SlotsType, (1, )],            (SlotsType(1, None),    False)),
            ([PropsType, (1, )],            (PropsType(1, None),    False)),
            ([TupleType, {"field1": 1}],    (TupleType(1, None),    False)),
            ([SlotsType, {}],               (SlotsType(None, None), False)),
        ]
        logger.info("Verifying %s.", NAME(FUNC))
        for args, (expected, expected_errors) in DATAS:
            logger.debug("Verifying %s.", NAME(FUNC, *args))
            received, errors = FUNC(*args)
            self.assertIsInstance(errors, list, ERR(FUNC, *args))
            if args[0] is TupleType: self.assertEqual(received, expected, ERR(FUNC, *args))
            else: self.assertIsInstance(received, type(expected), ERR(FUNC, *args))
            if expected_errors: self.assertTrue(errors, ERR(FUNC, *args))


    def test_is_dataobject(self):
        """Tests util.is_dataobject()."""
        DATAS = [  # [(input, expected), ]
            ({},              False),
            ((),              False),
            ([],              False),
            (TupleType(1, 2), True),
            (PlainType(),     True),
            (PropsType(),     True),
            (SlotsType(),     True),
        ]
        self.verify_function(dblite.util.is_dataobject, DATAS)


    def test_is_namedtuple(self):
        """Tests util.is_namedtuple()."""
        DATAS = [  # [(input, expected), ]
            ({},              False),
            ((),              False),
            (TupleType(1, 2), True),
        ]
        self.verify_function(dblite.util.is_namedtuple, DATAS)


    def test_json_dumps_loads(self):
        """Tests util.json_dumps() and .json_loads()."""
        FUNC1, FUNC2 = dblite.util.json_dumps, dblite.util.json_loads
        DATAS = [  # [(input for dumps(), expected from loads()), ]
            (None,                                   None),
            ({"a": decimal.Decimal("2"), "b": ()},   {"a": 2, "b": []}),
            ([decimal.Decimal("1.23")],              [1.23]),
            (set("a"),                               ["a"]),
            ({0: datetime.datetime(2024, 1, 30)},    {"0": parse_datetime("2024-01-30 00:00:00")}),
        ]
        logger.info("Verifying %s and %s.", NAME(FUNC1), NAME(FUNC2))
        for arg1, expected2 in DATAS:
            logger.debug("Verifying %s.", NAME(FUNC1, arg1))
            received1, type1 = FUNC1(arg1), type(None) if expected2 is None else six.text_type
            self.assertIsInstance(received1, type1, ERR(FUNC1, arg1))
            logger.debug("Verifying %s.", NAME(FUNC2, received1))
            received2 = FUNC2(received1)
            self.assertEqual(received2, expected2, ERR(FUNC2, received1))


    def test_keyvalues(self):
        """Tests util.keyvalues()."""
        namefmt = lambda x: x.upper()
        dct = dict(zip(CFIELDS, (1, 2)))
        dctvals = list(dct.items())
        DATAS = [  # [(([..args..], {..kwargs..}), expected), ]
            (([dct], {}), dctvals),
            (([collections.defaultdict(**dct)], {}),     dctvals),
            (([collections.OrderedDict(dct)],   {}),     dctvals),
            (([[1, 2]],                         {}),     [1, 2]),
            (([set("a")],                       {}),     ["a"]),
            (([TupleType(1, 2)],                {}),     dctvals),
            (([PlainType(1, 2)],                {}),     dctvals),
            (([SlotsType(1, 2)],                {}),     dctvals),
            (([PropsType(1, 2)],                {}),     dctvals),
            (([PropsType(1, 2)], {"namefmt": namefmt}),  [(k.upper(), v) for k, v in dct.items()]),
        ]
        self.verify_function(dblite.util.keyvalues, DATAS)


    def test_load_modules(self):
        """Tests util.load_modules()."""
        FUNC = dblite.util.load_modules
        logger.info("Verifying %s.", NAME(FUNC))
        received = FUNC()
        self.assertIsInstance(received, dict, ERR(FUNC))
        for k, v in received.items():
            self.assertIsInstance(k, six.string_types, ERR(FUNC))
            self.assertIsInstance(v, types.ModuleType, ERR(FUNC))


    def test_nameify(self):
        """Tests util.nameify()."""
        namefmt = lambda x: x.upper()
        DATAS = [  # [(([..args..], {..kwargs..}), expected), ]
            ((["value"],          {}),                                         "value"),
            (([123],              {}),                                         "123"),
            (([TupleType],        {}),                                         CNAME),
            (([PlainType],        {}),                                         CNAME),
            (([SlotsType],        {"namefmt": namefmt}),                       CNAME.upper()),
            (([PropsType],        {}),                                         CNAME),
            (([TupleType.field1], {"parent": TupleType}),                      "field1"),
            (([SlotsType.field1], {}),                                         "field1"),
            (([PropsType.field2], {"parent": PropsType, "namefmt": namefmt}),  "FIELD2"),
        ]
        self.verify_function(dblite.util.nameify, DATAS)


    def test_parse_datetime(self):
        """Tests util.parse_datetime()."""
        FUNC = dblite.util.parse_datetime
        DATAS = [  # [(input, (y, m, d, h, i, s, us, offset) or None if no datetime expected), ]
            ( "",                                 None),
            (b"",                                 None),
            ( "0000-01-30 12:13:14",              None),
            (b"2024-31-99T12:13:14",              None),
            ( "2024-01-30 12:13:14",              (2024, 1, 30, 12, 13, 14,      0,         0)),
            (b"2024-01-30 12:13:14.456789",       (2024, 1, 30, 12, 13, 14, 456789,         0)),
            ( "2024-01-30T12:13:14.456789",       (2024, 1, 30, 12, 13, 14, 456789,         0)),
            (b"2024-01-30 12:13:14.456789Z",      (2024, 1, 30, 12, 13, 14, 456789,         0)),
            ( "2024-01-30 12:13:14.456789+00:00", (2024, 1, 30, 12, 13, 14, 456789,         0)),
            (b"2024-01-30T12:13:14+04:30",        (2024, 1, 30, 12, 13, 14,      0,   4*60+30)),
            ( "2024-01-30 12:13:14.987654-11:23", (2024, 1, 30, 12, 13, 14, 987654, -11*60-23)),
        ]
        logger.info("Verifying %s.", NAME(FUNC))
        for arg, parts in DATAS:
            logger.debug("Verifying %s.", NAME(FUNC, arg))
            received = FUNC(arg)
            if parts is None: self.assertIs(received, arg, ERR(FUNC, arg))
            else:
                (y, m, d, h, i, s, us, offset) = parts
                self.assertIsInstance(received, datetime.datetime, ERR(FUNC, arg))
                self.assertEqual(received.timetuple()[:6], (y, m, d, h, i, s), ERR(FUNC, arg))
                self.assertEqual(received.microsecond, us, ERR(FUNC, arg))
                self.assertIsNot(received.tzinfo, None, ERR(FUNC, arg))
                self.assertEqual(received.utcoffset(), datetime.timedelta(minutes=offset),
                                 ERR(FUNC, arg))


    def verify_function(self, func, entries):
        """
        Tests given function with given pairs of (argument, expected).

        Argument may be ([..args..], {..kwargs..}).
        """
        logger.info("Verifying %s.", NAME(func))
        for arg, expected in entries:
            if isinstance(arg, tuple) and len(arg) == 2 \
            and isinstance(arg[0], list) and isinstance(arg[1], dict):
                args, kwargs = arg
            else: args, kwargs = [arg], {}
            logger.debug("Verifying %s.", NAME(func, *args, **kwargs))
            self.assertEqual(func(*args, **kwargs), expected, ERR(func, arg))


def NAME(func, *args, **kwargs):
    """Returns function name and module, with arguments if any."""
    argstr  = ", ".join(map(repr, args))
    argstr += ", " if args and kwargs else ""
    argstr += ", ".join("%s=%r" % (k, v) for k, v in kwargs.items())
    return ".".join(filter(bool, [func.__module__, func.__name__])) + "(%s)" % argstr


def ERR(func, *args, **kwargs):
    """Returns error message for function with full name and arguments."""
    return "Unexpected result from %s." % (NAME(func, *args, **kwargs))



if "__main__" == __name__:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s]\t[%(created).06f] [test_util] %(message)s"
    )
    unittest.main()
