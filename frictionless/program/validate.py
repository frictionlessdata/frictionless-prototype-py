import click
from sys import exit
import json as json_module
from pprint import pformat
from ..validate import validate
from .main import program


# NOTE: rewrite this function
@program.command(name="validate")
@click.argument("source", type=click.Path(), required=True)
@click.option("--source-type", type=str, help="Source type")
@click.option("--json", is_flag=True, help="Output report as JSON")
# Headers
@click.option("--headers", type=int, multiple=True, help="Headers")
# File
@click.option("--scheme", type=str, help="File scheme")
@click.option("--format", type=str, help="File format")
@click.option("--hashing", type=str, help="File hashing")
@click.option("--encoding", type=str, help="File encoding")
@click.option("--compression", type=str, help="File compression")
@click.option("--compression-path", type=str, help="File compression path")
# Schema
@click.option("--schema", type=click.Path(), help="Schema")
@click.option("--sync-schema", is_flag=True, help="Sync schema")
@click.option("--infer-type", type=str, help="Infer type")
@click.option("--infer-names", type=str, multiple=True, help="Infer names")
@click.option("--infer-sample", type=int, help="Infer sample")
@click.option("--infer-confidence", type=float, help="Infer confidence")
@click.option("--infer-missing-values", type=str, multiple=True, help="Infer missing")
# Validation
@click.option("--checksum-hash", type=str, help="Expected hash based on hashing option")
@click.option("--checksum-bytes", type=int, help="Expected size in bytes")
@click.option("--checksum-rows", type=int, help="Expected size in bytes")
@click.option("--pick-errors", type=str, multiple=True, help="Pick errors")
@click.option("--skip-errors", type=str, multiple=True, help="Skip errors")
@click.option("--limit-errors", type=int, help="Limit errors")
@click.option("--limit-memory", type=int, help="Limit memory")
# Package/Resource
@click.option("--noinfer", type=bool, help="Validate metadata as it is")
def program_validate(source, *, headers, source_type, json, **options):
    for key, value in list(options.items()):
        if not value:
            del options[key]
        elif isinstance(value, tuple):
            options[key] = list(value)
    report = validate(source, source_type=source_type, **options)
    print_report(report, json=json)
    exit(int(not report["valid"]))


# Internal


# NOTE: rewrite this function
def print_report(report, output=None, json=False):
    if json:
        return click.secho(json_module.dumps(report, indent=2, ensure_ascii=False))
    color = "green" if report.valid else "red"
    report = report.to_dict()
    tables = report.pop("tables")
    errors = report.pop("errors")
    click.secho("REPORT", bold=True)
    click.secho("=" * 7, bold=True)
    click.secho(pformat(report), fg=color, bold=True)
    if errors:
        click.secho("-" * 9, bold=True)
    for error in errors:
        click.secho("Error: %s" % error.message, fg="yellow")
    for table_number, table in enumerate(tables, start=1):
        click.secho("\nTABLE [%s]" % table_number, bold=True)
        click.secho("=" * 9, bold=True)
        color = "green" if table["valid"] else "red"
        errors = table.pop("errors")
        click.secho(pformat(table), fg=color, bold=True)
        if errors:
            click.secho("-" * 9, bold=True)
        for error in errors:
            click.secho(
                "[%s, %s] [%s] %s"
                % (
                    error.get("rowPosition"),
                    error.get("fieldPosition"),
                    error.code,
                    error.message,
                )
            )
