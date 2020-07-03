import os
import boto3
import pytest
import string
import random
import subprocess

#  from moto import mock_s3
from frictionless import Table, exceptions

# Setup

S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"] = "http://localhost:5000"


# Read

# https://github.com/frictionlessdata/tabulator-py/issues/271
@pytest.mark.skip
def test_table_s3(s3_client, bucket):

    # Upload a file
    s3_client.put_object(
        ACL="private",
        Body=open("data/table.csv", "rb"),
        Bucket=bucket,
        ContentType="text/csv",
        Key="table.csv",
    )

    # Check the file
    with Table("s3://%s/table.csv" % bucket) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


# https://github.com/frictionlessdata/tabulator-py/issues/271
@pytest.mark.skip
def test_table_s3_endpoint_url(s3_client, bucket):

    # Upload a file
    s3_client.put_object(
        ACL="private",
        Body=open("data/table.csv", "rb"),
        Bucket=bucket,
        ContentType="text/csv",
        Key="table.csv",
    )

    # Check the file
    with Table("s3://%s/table.csv" % bucket, s3_endpoint_url=S3_ENDPOINT_URL) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


# https://github.com/frictionlessdata/tabulator-py/issues/271
@pytest.mark.skip
def test_table_s3_non_existent_file(s3_client, bucket):
    with pytest.raises(exceptions.IOError):
        Table("s3://%s/table.csv" % bucket).open()


# Fixtures


@pytest.fixture(scope="module")
def s3_client():
    subprocess.Popen(
        "moto_server", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    s3_client = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)
    yield s3_client
    os.system("pkill moto_server")


@pytest.fixture
def bucket(s3_client):
    bucket = "bucket_%s" % "".join(random.choice(string.digits) for _ in range(16))
    s3_client.create_bucket(Bucket=bucket, ACL="public-read")
    return bucket
