import json

import polars as pl


def shapes_geojson(*shape_id, dir = "./data/gtfs/", write = False, out = "./data/gtfs/", test = False, regex = False):
    """
    converts shapes.txt into geojson\n

    **dir**: *str*, gtfs directory. \n
    **write**: *bool*, to output to file. \n
    **out**: *str*, output file dir. \n
    **test**: *bool*, returns first 2 shape objs.\n
    **shape_id**: *str*, filters to shape_ids contaning string.\n
    **regex**: *bool*, to use regular expressions with shape_id. takes one input.\n
    **returns**: json obj (str)
     
    """
    shape = pl.scan_csv(f"{dir}/shapes.txt", schema_overrides={
        "shape_id":pl.String,
        "shape_pt_lat":pl.Float64,
        "shape_pt_lon":pl.Float64,
        "shape_pt_sequence":pl.Int64

    })

    shape = shape.with_columns(pl.concat_list(pl.col("shape_pt_lon"), pl.col("shape_pt_lat")).alias("coord"))
    shape = shape.group_by("shape_id").agg(pl.col("coord").sort_by("shape_pt_sequence").alias("coordinates"))

    if shape_id:
        if regex:
            shape = shape.filter(pl.col("shape_id").str.contains(shape_id[0]).alias("filter"))
        else:
            shape = shape.filter(pl.col("shape_id").str.contains_any(list(shape_id)).alias("filter"))

    shape = shape.with_columns(
        pl.struct(
            pl.lit("Feature").alias("type"),
            pl.struct(
                pl.lit("LineString").alias("type"),
                pl.col("coordinates"),
                ).alias("geometry"),
            pl.struct(pl.col("shape_id").alias("route")).alias("properties"),
        ).alias("geojson")
    )

    if test:
        features = shape.sort("shape_id").select("geojson").head(2).collect().to_series().to_list()
    else:
        features = shape.sort("shape_id").select("geojson").collect().to_series().to_list()

    if write:
        with open(f"{out}/shape_geojson.json", "w") as f:
            json.dump({"type":"FeatureCollection", "features":features}, f)

    return json.dumps({"type":"FeatureCollection", "features":features})