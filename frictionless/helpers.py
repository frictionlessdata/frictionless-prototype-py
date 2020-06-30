import re
import os
import datetime
import stringcase
from copy import copy
from importlib import import_module
from urllib.parse import urlparse, parse_qs
from _thread import RLock  # type: ignore
from . import exceptions
from . import config


# General


def apply_function(function, descriptor):
    options = create_options(descriptor)
    return function(**options)


def create_options(descriptor):
    return {stringcase.snakecase(key): value for key, value in descriptor.items()}


def create_descriptor(**options):
    return {stringcase.camelcase(key): value for key, value in options.items()}


def stringify_headers(cells):
    return ["" if cell is None else str(cell).strip() for cell in cells]


def filter_cells(cells, field_positions):
    result = []
    for field_position, cell in enumerate(cells, start=1):
        if field_position in field_positions:
            result.append(cell)
    return result


def compile_regex(items):
    if items is not None:
        result = []
        for item in items:
            if isinstance(item, str) and item.startswith("<regex>"):
                item = re.compile(item.replace("<regex>", ""))
            result.append(item)
        return result


def ensure_dir(path):
    dirpath = os.path.dirname(path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath)


def parse_hashing_algorithm(hash):
    if not hash:
        return "md5"
    parts = hash.split(":", maxsplit=1)
    return parts[0] if len(parts) > 1 else "md5"


def parse_hashing_digest(hash):
    if not hash:
        return ""
    parts = hash.split(":", maxsplit=1)
    return parts[1] if len(parts) > 1 else hash


def reset_cached_properties(obj):
    for name, attr in type(obj).__dict__.items():
        if name in obj.__dict__:
            if isinstance(attr, cached_property):
                obj.__dict__.pop(name)


def infer_source_type(source):
    source_type = "table"
    if isinstance(source, dict):
        if source.get("fields") is not None:
            source_type = "schema"
        if source.get("path") is not None or source.get("data") is not None:
            source_type = "resource"
        if source.get("resources") is not None:
            source_type = "package"
        if source.get("tasks") is not None:
            source_type = "inquiry"
    if isinstance(source, str):
        if source.endswith("schema.json"):
            source_type = "schema"
        if source.endswith("resource.json"):
            source_type = "resource"
        if source.endswith("datapackage.json"):
            source_type = "package"
        if source.endswith("inquiry.json"):
            source_type = "inquiry"
    return source_type


# TODO: move to file
def infer_source_scheme_and_format(source):
    if hasattr(source, "read"):
        return ("stream", None)
    if not isinstance(source, str):
        return (None, "inline")
    if "docs.google.com/spreadsheets" in source:
        if "export" not in source and "pub" not in source:
            return (None, "gsheet")
        elif "csv" in source:
            return ("https", "csv")
    parsed = urlparse(source)
    scheme = parsed.scheme.lower()
    if len(scheme) < 2:
        scheme = config.DEFAULT_SCHEME
    format = os.path.splitext(parsed.path or parsed.netloc)[1][1:].lower() or None
    if format is None:
        # Test if query string contains a "format=" parameter.
        query_string = parse_qs(parsed.query)
        query_string_format = query_string.get("format")
        if query_string_format is not None and len(query_string_format) == 1:
            format = query_string_format[0]
    if parsed.path.endswith("datapackage.json"):
        return (None, "package")
    return (scheme, format)


def create_lookup(resource, *, package=None):
    lookup = {}
    for fk in resource.schema.foreign_keys:
        source_name = fk["reference"]["resource"]
        source_key = tuple(fk["reference"]["fields"])
        source_res = package.get_resource(source_name) if source_name else resource
        if source_name != "" and not package:
            continue
        lookup.setdefault(source_name, {})
        if source_key in lookup[source_name]:
            continue
        lookup[source_name][source_key] = set()
        if not source_res:
            continue
        try:
            # Current version of tableschema/datapackage raises cast errors
            # In the future this code should use not raising iterator
            for keyed_row in source_res.iter(keyed=True):
                cells = tuple(keyed_row[field_name] for field_name in source_key)
                if set(cells) == {None}:
                    continue
                lookup[source_name][source_key].add(cells)
        except Exception:
            pass
    return lookup


def get_current_memory_usage():
    # Current memory usage of the current process in MB
    # This will only work on systems with a /proc file system (like Linux)
    # https://stackoverflow.com/questions/897941/python-equivalent-of-phps-memory-get-usage
    try:
        with open("/proc/self/status") as status:
            for line in status:
                parts = line.split()
                key = parts[0][2:-1].lower()
                if key == "rss":
                    return int(parts[1]) / 1000
    except Exception:
        pass


class Timer:
    def __init__(self):
        self.__initial = datetime.datetime.now()

    def get_time(self):
        current = datetime.datetime.now()
        return round((current - self.__initial).total_seconds(), 3)


# Compatability


def translate_headers(headers):
    # frictionless: [2, 3, 4] (pandas-like)
    # tabulator: [2, 4] (range-like)
    if headers and isinstance(headers, list):
        if len(headers) == 1:
            return headers[0]
        if len(headers) > 1:
            headers = [headers[0], headers[-1]]
            for header in headers:
                assert isinstance(header, int)
    return headers


def translate_pick_fields(pick_fields):
    for index, item in enumerate(pick_fields or []):
        if isinstance(item, str) and item.startswith("<regex>"):
            pick_fields[index] = {"type": "regex", "value": item.replace("<regex>", "")}
    return pick_fields


def translate_skip_fields(skip_fields):
    for index, item in enumerate(skip_fields or []):
        if isinstance(item, str) and item.startswith("<regex>"):
            skip_fields[index] = {"type": "regex", "value": item.replace("<regex>", "")}
    return skip_fields


def translate_pick_rows(pick_rows):
    for index, item in enumerate(pick_rows or []):
        if isinstance(item, str) and item.startswith("<regex>"):
            pick_rows[index] = {"type": "regex", "value": item.replace("<regex>", "")}
    return pick_rows


def translate_skip_rows(skip_rows):
    for index, item in enumerate(skip_rows or []):
        if isinstance(item, str) and item.startswith("<regex>"):
            skip_rows[index] = {"type": "regex", "value": item.replace("<regex>", "")}
        if isinstance(item, str) and item.startswith("<blank>"):
            skip_rows[index] = {"type": "preset", "value": "blank"}
    return skip_rows


def translate_dialect(dialect):
    options = {
        stringcase.lowercase(key): dialect.pop(key)
        for key in [
            "doubleQuote",
            "escapeChar",
            "lineTerminator",
            "quoteChar",
            "skipInitialSpace",
        ]
        if key in dialect
    }
    options.pop("header", None)
    options.pop("caseSensitiveHeader", None)
    options.update(create_options(dialect))
    return options


def translate_control(control):
    return create_options(control)


# Tabulator


def detect_scheme_and_format(source):
    """Detect scheme and format based on source and return as a tuple.

    Scheme is a minimum 2 letters before `://` (will be lower cased).
    For example `http` from `http://example.com/table.csv`

    """
    # TODO: remove
    from . import config

    # Scheme: stream
    if hasattr(source, "read"):
        return ("stream", None)

    # Format: inline
    if not isinstance(source, str):
        return (None, "inline")

    # Format: gsheet
    if "docs.google.com/spreadsheets" in source:
        if "export" not in source and "pub" not in source:
            return (None, "gsheet")
        elif "csv" in source:
            return ("https", "csv")

    # Format: sql
    for sql_scheme in config.SQL_SCHEMES:
        if source.startswith("%s://" % sql_scheme):
            return (None, "sql")

    # General
    parsed = urlparse(source)
    scheme = parsed.scheme.lower()
    if len(scheme) == 1:
        scheme = None
    format = os.path.splitext(parsed.path or parsed.netloc)[1][1:].lower() or None
    if format is None:
        # Test if query string contains a "format=" parameter.
        query_string = parse_qs(parsed.query)
        query_string_format = query_string.get("format")
        if query_string_format is not None and len(query_string_format) == 1:
            format = query_string_format[0]

    # Format: datapackage
    if parsed.path.endswith("datapackage.json"):
        return (None, "datapackage")

    return (scheme, format)


def detect_html(text):
    """Detect if text is HTML.
    """
    pattern = re.compile("\\s*<(!doctype|html)", re.IGNORECASE)
    return bool(pattern.match(text))


def reset_stream(stream):
    """Reset stream pointer to the first element.

    If stream is not seekable raise Exception.

    """
    try:
        position = stream.tell()
    except Exception:
        position = True
    if position != 0:
        try:
            stream.seek(0)
        except Exception:
            message = "It's not possible to reset this stream"
            raise exceptions.TabulatorException(message)


def requote_uri(uri):
    """Requote uri if it contains non-ascii chars, spaces etc.
    """
    # To reduce tabulator import time
    import requests.utils

    return requests.utils.requote_uri(uri)


def import_attribute(path):
    """Import attribute by path like `package.module.attribute`
    """
    module_name, attribute_name = path.rsplit(".", 1)
    module = import_module(module_name)
    attribute = getattr(module, attribute_name)
    return attribute


def extract_options(options, names):
    """Return options for names and remove it from given options in-place.
    """
    result = {}
    for name, value in copy(options).items():
        if name in names:
            result[name] = value
            del options[name]
    return result


def stringify_value(value):
    """Convert any value to string.
    """
    if value is None:
        return ""
    isoformat = getattr(value, "isoformat", None)
    if isoformat is not None:
        value = isoformat()
    return type("")(value)


# Backports


class cached_property:
    def __init__(self, func):
        self.func = func
        self.attrname = None
        self.__doc__ = func.__doc__
        self.lock = RLock()

    def __set_name__(self, owner, name):
        if self.attrname is None:
            self.attrname = name
        elif name != self.attrname:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                f"({self.attrname!r} and {name!r})."
            )

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        if self.attrname is None:
            raise TypeError(
                "Cannot use cached_property instance without calling __set_name__ on it."
            )
        try:
            cache = instance.__dict__
        except AttributeError:  # not all objects have __dict__ (e.g. class defines slots)
            msg = (
                f"No '__dict__' attribute on {type(instance).__name__!r} "
                f"instance to cache {self.attrname!r} property."
            )
            raise TypeError(msg) from None
        val = cache.get(self.attrname, _NOT_FOUND)
        if val is _NOT_FOUND:
            with self.lock:
                # check if another thread filled cache while we awaited lock
                val = cache.get(self.attrname, _NOT_FOUND)
                if val is _NOT_FOUND:
                    val = self.func(instance)
                    try:
                        cache[self.attrname] = val
                    except TypeError:
                        msg = (
                            f"The '__dict__' attribute on {type(instance).__name__!r} instance "
                            f"does not support item assignment for caching {self.attrname!r} property."
                        )
                        raise TypeError(msg) from None
        return val


_NOT_FOUND = object()
