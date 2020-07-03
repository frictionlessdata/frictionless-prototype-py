import re
import os
import chardet
import datetime
import stringcase
from urllib.parse import urlparse, parse_qs
from _thread import RLock  # type: ignore
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


def copy_merge(source, patch):
    source = source.copy()
    source.update(patch)
    return source


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


def detect_encoding(sample):
    result = chardet.detect(sample)
    confidence = result["confidence"] or 0
    encoding = result["encoding"] or config.DEFAULT_ENCODING
    if confidence < config.DEFAULT_INFER_ENCODING_CONFIDENCE:
        encoding = config.DEFAULT_ENCODING
    if encoding == "ascii":
        encoding = config.DEFAULT_ENCODING
    return encoding


def detect_source_type(source):
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


def detect_source_scheme_and_format(source):
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
        self.__start = datetime.datetime.now()
        self.__stop = None

    @property
    def time(self):
        if not self.__stop:
            self.__stop = datetime.datetime.now()
        return round((self.__stop - self.__start).total_seconds(), 3)


# Collections


class ControlledDict(dict):
    def __init__(self, source, *, on_change):
        super().__init__(source)
        self.__on_change = on_change

    def __setitem__(self, *args, **kwargs):
        result = super().__setitem__(*args, **kwargs)
        self.__on_change()
        return result

    def __delitem__(self, *args, **kwargs):
        result = super().__delitem__(*args, **kwargs)
        self.__on_change()
        return result

    def clear(self, *args, **kwargs):
        result = super().clear(*args, **kwargs)
        self.__on_change()
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self.__on_change()
        return result

    def popitem(self, *args, **kwargs):
        result = super().popitem(*args, **kwargs)
        self.__on_change()
        return result

    def setdefault(self, *args, **kwargs):
        result = super().setdefault(*args, **kwargs)
        self.__on_change()
        return result

    def update(self, *args, **kwargs):
        result = super().update(*args, **kwargs)
        self.__on_change()
        return result


class ControlledList(list):
    def __init__(self, source, *, on_change):
        super().__init__(source)
        self.__on_change = on_change

    def __setitem__(self, *args, **kwargs):
        result = super().__setitem__(*args, **kwargs)
        self.__on_change()
        return result

    def __delitem__(self, *args, **kwargs):
        result = super().__delitem__(*args, **kwargs)
        self.__on_change()
        return result

    def append(self, *args, **kwargs):
        result = super().append(*args, **kwargs)
        self.__on_change()
        return result

    def clear(self, *args, **kwargs):
        result = super().clear(*args, **kwargs)
        self.__on_change()
        return result

    def extend(self, *args, **kwargs):
        result = super().extend(*args, **kwargs)
        self.__on_change()
        return result

    def insert(self, *args, **kwargs):
        result = super().insert(*args, **kwargs)
        self.__on_change()
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self.__on_change()
        return result

    def remove(self, *args, **kwargs):
        result = super().remove(*args, **kwargs)
        self.__on_change()
        return result


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
        val = cache.get(self.attrname, config.UNDEFINED)
        if val is config.UNDEFINED:
            with self.lock:
                # check if another thread filled cache while we awaited lock
                val = cache.get(self.attrname, config.UNDEFINED)
                if val is config.UNDEFINED:
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
