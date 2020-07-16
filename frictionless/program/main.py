import click
from .. import config


@click.group(name="frictionless")
@click.version_option(config.VERSION, message="%(version)s", help="Print version")
def program():
    pass
