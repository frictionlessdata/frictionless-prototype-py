import click
from ..transform import transform
from .main import program


@program.command(name="transform")
@click.argument("source", type=click.Path(), required=True)
def program_transform(source):
    transform(source)
