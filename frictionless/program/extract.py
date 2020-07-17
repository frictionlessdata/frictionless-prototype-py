import click
import json as json_module
from tabulate import tabulate
from ..extract import extract
from .main import program


@program.command(name="extract")
@click.argument("source", type=click.Path(), nargs=-1, required=True)
@click.option("--source-type", type=str, help="Source type")
@click.option("--json", is_flag=True, help="Output report as JSON")
# File
@click.option("--scheme", type=str, help="File scheme")
@click.option("--format", type=str, help="File format")
@click.option("--hashing", type=str, help="File hashing")
@click.option("--encoding", type=str, help="File encoding")
@click.option("--compression", type=str, help="File compression")
@click.option("--compression-path", type=str, help="File compression path")
# Schema
@click.option("--sync-schema", is_flag=True, help="Sync schema")
@click.option("--infer-type", type=str, help="Infer type")
@click.option("--infer-names", type=str, multiple=True, help="Infer names")
@click.option("--infer-sample", type=int, help="Infer sample")
@click.option("--infer-confidence", type=float, help="Infer confidence")
@click.option("--infer-missing-values", type=str, multiple=True, help="Infer missing")
def program_extract(source, *, source_type, json, **options):
    for key, value in list(options.items()):
        if not value:
            del options[key]
        elif isinstance(value, tuple):
            options[key] = list(value)
    source = list(source) if len(source) > 1 else source[0]
    data = extract(source, source_type=source_type, **options)
    if data:
        if json:
            # TODO: fix the problem with objects serialization
            return click.secho(json_module.dumps(data, indent=2, ensure_ascii=False))
        if isinstance(data, list):
            return click.secho(tabulate(data, headers="keys"))
        for number, (name, rows) in enumerate(data.items(), start=1):
            if number != 1:
                click.secho("\n")
            click.secho(f"{name}\n", bold=True)
            click.secho(tabulate(rows, headers="keys"))