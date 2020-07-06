import pytest
import sqlite3
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


# Cleanups

cleanup_on_sigterm()
