import os
import io
import boto3
import requests.utils
from urllib.parse import urlparse
from ..controls import Control
from ..plugin import Plugin
from ..loader import Loader
from .. import config


# Plugin


class AwsPlugin(Plugin):
    def create_loader(self, file, *, control=None):
        if file.scheme == 's3':
            return S3Loader(file, control=control)


# Controls


class S3Control(Control):
    """S3 control representation

    # Arguments
        descriptor? (str|dict): descriptor
        endpoint_url? (string): endpoint_url

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {'endpointUrl': {'type': 'string'}},
    }

    def __init__(self, descriptor=None, endpoint_url=None):
        self.setdefined('endpointUrl', endpoint_url)
        super().__init(descriptor)

    @property
    def endpoint_url(self):
        return (
            self.get('endpointUrl')
            or os.environ.get('S3_ENDPOINT_URL')
            or config.S3_DEFAULT_ENDPOINT_URL
        )

    # Expand

    def expand(self):
        self.setdetault('endpointUrl', self.endpoint_url)


# Loaders


class S3Loader(Loader):
    Contol = S3Control
    network = True

    # Read

    def read_byte_stream_create(self):
        client = boto3.client('s3', endpoint_url=self.control.endpoint_url)
        source = requests.utils.requote_uri(self.file.source)
        parts = urlparse(source, allow_fragments=False)
        response = client.get_object(Bucket=parts.netloc, Key=parts.path[1:])
        # https://github.com/frictionlessdata/tabulator-py/issues/271
        byte_stream = io.BufferedRandom(io.BytesIO())
        byte_stream.write(response['Body'].read())
        byte_stream.seek(0)
        return byte_stream
