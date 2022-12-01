# -*- coding: utf-8 -*-
"""
Utility classes and functions.

------------------------------------------------------------------------------
This file is part of dblite - simple query interface for SQL databases.
Released under the MIT License.

@author      Erki Suurjaak
@created     28.11.2022
@modified    01.12.2022
------------------------------------------------------------------------------
"""
import datetime
import decimal
import glob
import importlib
import inspect
import json
import logging
import os
import re

import six

logger = logging.getLogger(__name__)


class StaticTzInfo(datetime.tzinfo):
    """datetime.tzinfo class representing a constant offset from UTC."""
    ZERO = datetime.timedelta(0)

    def __init__(self, name, delta):
        """Constructs a new static zone info, with specified name and time delta."""
        self._name    = name
        self._offset = delta

    def utcoffset(self, dt): return self._offset
    def dst(self, dt):       return self.ZERO
    def tzname(self, dt):    return self._name
    def __ne__(self, other): return not self.__eq__(other)
    def __repr__(self):      return "%s(%s)" % (self.__class__.__name__, self._name)
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._offset == other._offset
## UTC timezone singleton
UTC = StaticTzInfo("UTC", StaticTzInfo.ZERO)



def factory(ctor, data):
    """
    Returns object constructed with data dictionary.

    @param   ctor  callable like a class, declared args are matched case-insensitively
                   for positional arguments if keyword argument invocation fails
    @param   data  data dictionary with string keys
    @return        (result, [error strings])
    """
    errors = []
    try: return ctor(**data), []          # Constructed with keyword args as data keys-values
    except Exception as e: errors.append(e)
    try: return ctor(*data.values()), []  # Constructed with positional args as data values
    except Exception as e: errors.append(e)
    try: return ctor(data), []            # Constructed with data as single arg
    except Exception as e: errors.append(e)
    if is_namedtuple(ctor):               # Populate any missing fields with None
        try: return ctor(**dict({k: None for k in ctor._fields}, **data)), []
        except Exception as e: errors.append(e)
        try: return ctor(*map(data.get, ctor._fields)), []
        except Exception as e: errors.append(e)
    return data, errors


def is_dataobject(obj):
    """Returns whether input is a data object: namedtuple, or has attributes or slots."""
    if is_namedtuple(obj):
        return True  # collections.namedtuple
    if getattr(obj, "__slots__", None):
        return True  # __slots__
    if any(isinstance(v, property) for _, v in inspect.getmembers(type(obj))):
        return True  # Declared properties
    if getattr(obj, "__dict__", None):
        return True  # Plain object
    return False


def is_namedtuple(obj):
    """Returns whether input is a namedtuple class or instance."""
    return (isinstance(obj, tuple) or inspect.isclass(obj) and issubclass(obj, tuple)) \
           and hasattr(obj, "_asdict") and hasattr(obj, "_fields")


def json_dumps(data, indent=2, sort_keys=True):
    """
    Returns JSON string, with datetime types converted to ISO-8601 strings
    (in UTC if no timezone set), sets converted to lists,
    and Decimal objects converted to float or int. Returns None if data is None.
    """
    if data is None: return None
    def encoder(x):
        if isinstance(x,    set): return list(x)
        if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
            if x.tzinfo is None: x = x.replace(tzinfo=UTC)
            return x.isoformat()
        if isinstance(x, decimal.Decimal):
            return float(x) if x.as_tuple().exponent else int(x)
        return None
    return json.dumps(data, default=encoder, indent=indent, sort_keys=sort_keys)


def json_loads(s):
    """
    Returns deserialized JSON, with datetime/date strings converted to objects.

    Returns original input if loading as JSON failed.
    """
    def convert_recursive(data):
        """Converts ISO datetime strings to objects in nested dicts or lists."""
        result = []
        pairs = enumerate(data) if isinstance(data, list) \
                else data.items() if isinstance(data, dict) else []
        rgx = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(([+-]\d{2}:?\d{2})|Z)?$"
        for k, v in pairs:
            if isinstance(v, (dict, list)): v = convert_recursive(v)
            elif isinstance(v, six.string_types) and len(v) > 18 and re.match(rgx, v):
                v = parse_datetime(v)
            result.append((k, v))
        return [x for _, x in result] if isinstance(data, list) \
               else dict(result) if isinstance(data, dict) else data
    try:
        return None if s is None else json.loads(s, object_hook=convert_recursive)
    except Exception:
        fails = getattr(json_loads, "__fails", set())
        if hash(s) not in fails: # Avoid spamming logs
            logger.warning("Failed to parse JSON from %r.", s, exc_info=True)
            setattr(json_loads, "__fails", fails | set([hash(s)]))
        return s


def keyvalues(obj, namefmt=None):
    """
    Returns a list of keys and values, or [given object] if not applicable.

    @param   obj      mapping or namedtuple or object with attributes or slots
    @param   namefmt  function(key) to apply on extracted keys, if any
    @return           [(key, value)] if available,
                      else original argument as list if list/set/tuple,
                      else list with a single item
    """
    namefmt = namefmt if callable(namefmt) else lambda x: x
    if is_namedtuple(obj):
        return [(namefmt(k), getattr(obj, k)) for k in obj._fields]  # collections.namedtuple
    if getattr(obj, "__slots__", None):
        return [(namefmt(k), getattr(obj, k)) for k in obj.__slots__
                if hasattr(obj, k)]                                  # __slots__
    if any(isinstance(v, property) for _, v in inspect.getmembers(type(obj))):
        return [(namefmt(k), getattr(obj, k)) for k, v in inspect.getmembers(type(obj))
                if isinstance(v, property)]                          # Declared properties
    if getattr(obj, "__dict__", None):
        return [(namefmt(k), v) for k, v in vars(obj).items()]       # Plain object
    if isinstance(obj, six.moves.collections_abc.Mapping):
        return list(obj.items())                                     # dictionary
    return list(obj) if isinstance(obj, (list, set, tuple)) else [obj]


def load_modules():
    """Returns db engines loaded from file directory, as {name: module}."""
    result = {}
    for n in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "engines", "*"))):
        name = os.path.splitext(os.path.basename(n))[0]
        if name.startswith("__") or os.path.isfile(n) and not re.match(".*pyc?$", n) \
        or os.path.isdir(n) and not any(glob.glob(os.path.join(n, x)) for x in ("*.py", "*.pyc")):
            continue  # for n

        modulename = "%s.%s.%s" % (__package__, "engines", name)
        module = importlib.import_module(modulename)
        result[name] = module
    return result


def nameify(val, namefmt=None, parent=None):
    """
    Returns value as table or column name string, for use in SQL statements.

    @param   val      a primitive like string, or a named object like a class,
                      or a class property or member or data descriptor
    @param   namefmt  function(name) to apply on name extracted from class or object, if any
    @param   parent   the parent class object if value is a class member or property
    @return           string
    """
    namefmt = namefmt if callable(namefmt) else lambda x: x
    if inspect.isclass(val):
        return namefmt(val.__name__)
    if inspect.isdatadescriptor(val):
        if hasattr(val, "__name__"): return namefmt(val.__name__)  # __slots__ entry
        return next(namefmt(k) for k, v in inspect.getmembers(parent) if v is val)
    return val if isinstance(val, six.string_types) else six.text_type(val)


def parse_datetime(s):
    """
    Tries to parse string as ISO8601 datetime, returns input on error.
    Supports "YYYY-MM-DD[ T]HH:MM:SS(.micros)?(Z|[+-]HH(:MM)?)?".
    All returned datetimes are timezone-aware, falling back to UTC.
    """
    if len(s) < 18: return s
    rgx = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(([+-]\d{2}(:?\d{2})?)|Z)?$"
    result, match = s, re.match(rgx, s)
    if match:
        millis, _, offset, _ = match.groups()
        minimal = re.sub(r"\D", "", s[:match.span(2)[0]] if offset else s)
        fmt = "%Y%m%d%H%M%S" + ("%f" if millis else "")
        try:
            result = datetime.datetime.strptime(minimal, fmt)
            if offset: # Support timezones like 'Z' or '+03:00'
                hh, mm = map(int, [offset[1:3], offset[4:]])
                delta = datetime.timedelta(hours=hh, minutes=mm)
                if offset.startswith("-"): delta = -delta
                result = result.replace(tzinfo=StaticTzInfo(offset, delta))
        except ValueError: pass
    if isinstance(result, datetime.datetime) and result.tzinfo is None:
        result = result.replace(tzinfo=UTC) # Force UTC timezone on unaware values
    return result


__all__ = [
    "StaticTzInfo", "UTC",
    "factory", "is_dataobject", "is_namedtuple", "json_dumps", "json_loads",
    "keyvalues", "load_modules", "nameify", "parse_datetime",
]
