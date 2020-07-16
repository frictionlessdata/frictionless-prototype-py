import os
from frictionless import Pipeline, Package


# General


def test_ensure_dir(tmpdir):

    # Write
    pipeline = Pipeline(
        {
            "type": "dataflows",
            "steps": [
                {"type": "load", "body": {"loadSource": "data/table.csv"}},
                {"type": "set_type", "body": {"name": "id", "type": "string"}},
                {"type": "dump_to_path", "body": {"outPath": tmpdir}},
            ],
        }
    )
    pipeline.run()

    # Read
    package = Package(os.path.join(tmpdir, "datapackage.json"))
    assert package.get_resource("table").read_rows() == [
        {"id": "1", "name": "english"},
        {"id": "2", "name": "中国人"},
    ]
