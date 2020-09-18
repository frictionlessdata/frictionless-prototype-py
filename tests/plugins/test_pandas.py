from frictionless import Package

# Storage


def test_storage():

    # Export/Import
    source = Package("data/package-storage.json")
    print(source)
    #  dataframes = source.to_pandas()
    #  print(dataframes)
    #  target = Package.from_sql(engine=engine, prefix=prefix)

    # Compare meta (with expected discrepancies)
    #  source.get_resource("comment").schema.get_field("note").type = "string"
    #  source.get_resource("location").schema.get_field("geojson").type = "string"
    #  source.get_resource("location").schema.get_field("geopoint").type = "string"
    #  source.get_resource("structure").schema.get_field("object").type = "string"
    #  source.get_resource("structure").schema.get_field("array").type = "string"
    #  source.get_resource("temporal").schema.get_field("date_year").pop("format")
    #  source.get_resource("temporal").schema.get_field("duration").type = "string"
    #  source.get_resource("temporal").schema.get_field("year").type = "integer"
    #  source.get_resource("temporal").schema.get_field("yearmonth").type = "string"
    #  for resource in source.resources:
    #  assert resource.schema == target.get_resource(resource.name).schema

    # Compare data (with expected discrepancies)
    #  source.get_resource("temporal").schema.get_field("date_year").format = "%Y"
    #  for resource in source.resources:
    #  assert resource.read_rows() == target.get_resource(resource.name).read_rows()

    # Cleanup storage
    #  storage.delete_package(target.resource_names)
