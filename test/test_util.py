#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test utility functions.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     19.07.2023
@modified    19.07.2023
------------------------------------------------------------------------------
"""
import datetime
import logging
import unittest

import dblite

logger = logging.getLogger()


class TestUtil(unittest.TestCase):
    """Tests utility functions."""


    def __init__(self, *args, **kwargs):
        super(TestUtil, self).__init__(*args, **kwargs)
        self.maxDiff = None  # Full diff on assert failure
        try: unittest.util._MAX_LENGTH = 100000
        except Exception: pass


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
