import click
import simplejson
from pprint import pformat
from tabulate import tabulate
from ..validate import validate
from .main import program


# NOTE: rebase on tabulate?
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

    # Json
    if json:
        return click.secho(simplejson.dumps(report, indent=2, ensure_ascii=False))

    # Report
    if report.errors:
        content = []
        click.secho("general", bold=True)
        click.secho("")
        for error in report.errors:
            content.append([error.code, error.message])
        click.secho(tabulate(content, headers=["code", "message"]))

    # Tables
    for table in report.tables:
        prefix = "valid" if table.valid else "invalid"
        click.secho(f"[{prefix}] {table.path}", bold=True)
        if table.errors:
            click.secho("")
            content = []
            for error in table.errors:
                content.append(
                    [
                        error.get("rowPosition", "-"),
                        error.get("fieldPosition", "-"),
                        error.code,
                        error.message,
                    ]
                )
            click.secho(tabulate(content, headers=["row", "field", "code", "message"]))

    # Retcode
    exit(int(not report.valid))
