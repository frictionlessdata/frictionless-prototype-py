import io
import os
import json


# Helpers


def read_asset(*paths):
    dirname = os.path.dirname(__file__)
    return io.open(os.path.join(dirname, 'assets', *paths)).read().strip()


# General

VERSION = read_asset('VERSION')
GEOJSON_PROFILE = json.loads(read_asset('profiles', 'geojson.json'))
INQUIRY_PROFILE = json.loads(read_asset('profiles', 'inquiry.json'))
REPORT_PROFILE = json.loads(read_asset('profiles', 'report.json'))
SCHEMA_PROFILE = json.loads(read_asset('profiles', 'schema.json'))
SQL_SCHEMES = ['firebird', 'mssql', 'mysql', 'oracle', 'postgresql', 'sqlite', 'sybase']
REMOTE_SCHEMES = ['http', 'https', 'ftp', 'ftps']
COMPRESSION_FORMATS = ['zip', 'gz']
INFER_ENCODING_VOLUME = 10000
INFER_ENCODING_CONFIDENCE = 0.5
INFER_DIALECT_VOLUME = 100

# Defaults

DEFAULT_SCHEME = 'file'
DEFAULT_FORMAT = 'csv'
DEFAULT_HASHING = 'md5'
DEFAULT_ENCODING = 'utf-8'
DEFAULT_COMPRESSION = 'no'
DEFAULT_HEADERS_ROW = 1
DEFAULT_HEADERS_JOINER = ' '
DEFAULT_MISSING_VALUES = ['']
DEFAULT_LIMIT_MEMORY = 1000
DEFAULT_INFER_VOLUME = 100
DEFAULT_INFER_CONFIDENCE = 0.9
DEFAULT_HTTP_TIMEOUT = 10
DEFAULT_HTTP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/54.0.2840.87 Safari/537.36'
    )
}
