import json
from .helpers import read_asset


# General

VERSION = read_asset('VERSION')
GEOJSON_PROFILE = json.loads(read_asset('profiles', 'geojson.json'))
INQUIRY_PROFILE = json.loads(read_asset('profiles', 'inquiry.json'))
REPORT_PROFILE = json.loads(read_asset('profiles', 'report.json'))
SCHEMA_PROFILE = json.loads(read_asset('profiles', 'schema.json'))
REMOTE_SCHEMES = ['http', 'https', 'ftp', 'ftps']
MISSING_VALUES = ['']
INFER_CONFIDENCE = 0.9
INFER_SAMPLE = 100
LIMIT_MEMORY = 1000
HEADERS_ROW = 1
HEADERS_JOINER = ' '

# Tabulator
# TODO: rework

DEFAULT_SCHEME = 'file'
DEFAULT_ENCODING = 'utf-8'
DEFAULT_SAMPLE_SIZE = 100
DEFAULT_BYTES_SAMPLE_SIZE = 10000
SUPPORTED_COMPRESSION = ['zip', 'gz']
SUPPORTED_HASHING_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512']
ENCODING_CONFIDENCE = 0.5
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) '
    + 'AppleWebKit/537.36 (KHTML, like Gecko) '
    + 'Chrome/54.0.2840.87 Safari/537.36'
}
CSV_SAMPLE_LINES = 100
# http://docs.sqlalchemy.org/en/latest/dialects/index.html
SQL_SCHEMES = ['firebird', 'mssql', 'mysql', 'oracle', 'postgresql', 'sqlite', 'sybase']
S3_DEFAULT_ENDPOINT_URL = 'https://s3.amazonaws.com'

# Loaders

LOADERS = {
    's3': 'frictionless.plugins.aws.AWSLoader',
    'file': 'frictionless.loaders.local.LocalLoader',
    'http': 'frictionless.loaders.remote.RemoteLoader',
    'https': 'frictionless.loaders.remote.RemoteLoader',
    'ftp': 'frictionless.loaders.remote.RemoteLoader',
    'ftps': 'frictionless.loaders.remote.RemoteLoader',
    'stream': 'frictionless.loaders.stream.StreamLoader',
    'text': 'frictionless.loaders.text.TextLoader',
}

# Parsers

PARSERS = {
    'csv': 'frictionless.plugins.csv.CSVParser',
    'datapackage': 'frictionless.plugins.datapackage.DataPackageParser',
    'gsheet': 'frictionless.plugins.gsheet.GsheetParser',
    'html': 'frictionless.plugins.html.HTMLTableParser',
    'inline': 'frictionless.parsers.inline.InlineParser',
    'json': 'frictionless.plugins.json.JSONParser',
    'jsonl': 'frictionless.plugins.json.NDJSONParser',
    'ndjson': 'frictionless.plugins.json.NDJSONParser',
    'ods': 'frictionless.plugins.ods.ODSParser',
    'sql': 'frictionless.plugins.sql.SQLParser',
    'tsv': 'frictionless.plugins.tsv.TSVParser',
    'xls': 'frictionless.plugins.excel.XLSParser',
    'xlsx': 'frictionless.plugins.excel.XLSXParser',
}

# Writers

WRITERS = {
    'csv': 'frictionless.plugins.csv.CSVWriter',
    'json': 'frictionless.plugins.json.JSONWriter',
    'xlsx': 'frictionless.plugins.excel.XLSXWriter',
    'sql': 'frictionless.plugins.sql.SQLWriter',
}
