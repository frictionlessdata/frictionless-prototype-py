import pytest
import sqlite3
import httpretty
from functools import partial
from pytest_cov.embed import cleanup_on_sigterm


# Fixtures


@pytest.fixture
def database_url(tmpdir):
    path = str(tmpdir.join("database.db"))
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute('INSERT INTO data VALUES (1, "english"), (2, "中国人")')
    conn.commit()
    yield "sqlite:///%s" % path
    conn.close()


@pytest.fixture
def patch_get():
    httpretty.enable()
    yield partial(httpretty.register_uri, httpretty.GET)
    httpretty.disable()
    httpretty.reset()


# Cleanups

cleanup_on_sigterm()
