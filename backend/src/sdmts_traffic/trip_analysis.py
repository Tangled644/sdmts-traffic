import polars as pl


def trip_start_end(dir:str = "./data/gtfs/"):
    """
    extracts start and end time for trips in trips.txt. \n

    **dir**: *str*, gtfs directory. \n
    **returns**: polars lazy object by "trip_id"
    
    """
    trips = pl.scan_csv(f"{dir}/stop_times.txt").select([
        "trip_id", "stop_id", "departure_time", "shape_dist_traveled"
    ])

    routes = pl.scan_csv(f"{dir}/trips.txt", schema_overrides={
        "route_id": pl.String,
        "trip_id": pl.Int64,
    }).select(["route_id", "trip_id"]).unique()

    # uses shape_dist_travelled to determine first/last stop and time. 
    # SDMTS has reliable shapes.

    trips = trips.join(routes, on="trip_id").group_by("trip_id").agg([
        pl.col("route_id").first(),
        pl.col("shape_dist_traveled").max().alias("distance"),

        pl.col("stop_id").sort_by("shape_dist_traveled").first().alias("start_stop_id"),
        pl.col("stop_id").sort_by("shape_dist_traveled").last().alias("end_stop_id"),
        
        pl.col("departure_time").sort_by("shape_dist_traveled").first().alias("start_time"),
        pl.col("departure_time").sort_by("shape_dist_traveled").last().alias("end_time"),
    ])


    return trips

def merge_routes_time(dir:str = "./data/gtfs/"):
    """
    uses shape_id of trips.txt to merge trips. \n
    **dir**: *str*, uncompressed gtfs folder. \n
    *returns*: polars lazy object.
    """

    trips = pl.scan_csv(f"{dir}trips.txt", schema_overrides={
        "route_id": pl.String,
        "trip_id": pl.Int64,
        "trip_headsign": pl.String,
        "block_id": pl.String,

    })

    timed_trips = trip_start_end(dir=dir)
    trips = trips.join(timed_trips, on="trip_id").select([
        pl.col("route_id"),
        pl.col("shape_id"),
        pl.col("trip_id"),
        pl.col("trip_headsign"),
        pl.col("distance"),
        pl.col("start_stop_id"),
        pl.col("end_stop_id"),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("service_id")
    ])

    return trips.sort(["shape_id", "start_time"])

def add_day_of_week(lf:pl.LazyFrame, dir:str = "./data/gtfs/"):
    """
    Adds a column indicating days of week the trip_id operates.\n
    Merges trips.txt and calendar.txt on service_id.\n

    **lf**: *lazyframe* input, from merge_routes_time()\n
    **dir**: *str*, uncompressed gtfs folder.\n
    """

    schedule = pl.scan_csv(f"{dir}/calendar.txt").cast({
        "monday": pl.Boolean,
        "tuesday": pl.Boolean,
        "wednesday": pl.Boolean,
        "thursday": pl.Boolean,
        "friday": pl.Boolean,
        "saturday": pl.Boolean,
        "sunday": pl.Boolean,
        "start_date":pl.String,
        "end_date":pl.String
    }).select([
        "service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"
    ])
    schedule = schedule.with_columns([
        pl.all_horizontal("monday", "tuesday", "wednesday", "thursday", "friday").alias("weekday"),
        pl.any_horizontal("saturday", "sunday").alias("weekend")
    ])
    schedule = lf.join(schedule, on="service_id").with_columns((pl.col("end_date").str.to_date(r"%Y%m%d") - pl.col("start_date").str.to_date(r"%Y%m%d")).alias("date_range"))

    return schedule.sort(["shape_id", "start_time"])



    

def calc_trip_time(lf:pl.LazyFrame, time_bucket:str="15m", start_time:str="start_time", end_time:str="end_time"):
    """
    calculates trip time based on start and end time of input df\n
    Assumes output comes from merge_routes_time() or _add_day_of_week.\n
    time columns should be of str, "HH:MM:SS", HH>23 ok (gtfs standard).\n
    adds trip_sec: *int*, trip_time: *pl.Time*).\n


    **lf**: *lazyframe*, polars lazy object, from merge_routes_time()\n
    **start_time**: *str*, start time column name\n
    **end_time**: *str*, end time column name.\n
    **returns**: lazy object.

    """
    time_pattern = r"(?P<h>\d+):(?P<m>\d+):(?P<s>\d+)"

    # seperates str, before composing seconds since last midnight. 
    shapes = lf.with_columns([
        pl.col(start_time).str.extract_groups(time_pattern).alias("hms_start"),
        pl.col(end_time).str.extract_groups(time_pattern).alias("hms_end"),
    ])

    shapes = shapes.with_columns([
        (
            pl.col("hms_start").struct.field("h").cast(pl.Int64) * 3600 +
            pl.col("hms_start").struct.field("m").cast(pl.Int64) * 60 +
            pl.col("hms_start").struct.field("s").cast(pl.Int64)
        ).alias("start_sec"),
        (
            pl.col("hms_end").struct.field("h").cast(pl.Int64) * 3600 +
            pl.col("hms_end").struct.field("m").cast(pl.Int64) * 60 +
            pl.col("hms_end").struct.field("s").cast(pl.Int64)
        ).alias("end_sec"),
    ]).drop(["hms_start", "hms_end"])

    # finds total travel time, pl.Time uses nanoseconds, pl.Datetime uses microseconds.
    shapes = shapes.with_columns([
        (pl.col("end_sec") - pl.col("start_sec")).alias("trip_sec"),
        ((pl.col("end_sec") - pl.col("start_sec")) * 1000000000).cast(pl.Time()).alias("trip_time"),
        (pl.col("start_sec") * 1000000).cast(pl.Datetime()).dt.round(time_bucket).dt.time().alias("start_bucket"),
        (pl.col("end_sec") * 1000000).cast(pl.Datetime()).dt.round(time_bucket).dt.time().alias("end_bucket"),

    ]).drop(["start_sec", "end_sec"])
 
    return shapes.sort("shape_id")

def calc_route_time_diff(lf:pl.LazyFrame, keep_holiday:bool=False, keep_cols:bool = True):
    """
    calculates travel time differences among the same shape_id\n

    **lf**: *lazyframe*, polars lazy object, from calc_trip_time()\n
    **keep_cols**: *bool*, keeps all columns post-group.\n
    **returns**: lazy object.
    """
    if keep_holiday:
        trips = lf
    else:
    # exclude temporary routes, such as weekend routes for weekday holidays
    # may exclude starts and ends of service that do not coincide with service changes
    # ex: school tripper service
        trips = lf.filter(pl.col("date_range").dt.total_days() > 28)

    # trips = trips.filter(pl.col("route_id") == "910")

    trips = trips.group_by("shape_id", "service_id").agg(
        pl.col("route_id").first(),
        pl.col("trip_headsign").first(),
        pl.struct(pl.col("start_bucket"), pl.col("end_bucket"), pl.col("start_time"), pl.col("end_time"), pl.col("trip_time"), (pl.col("trip_sec") / pl.col("trip_sec").min()).round(4).alias("trip_delay_ratio")).alias("time_duration_bucket"),
        pl.col("distance").first(),
        pl.col("start_stop_id").first(),
        pl.col("end_stop_id").first(),
        pl.col("start_time"),
        pl.col("end_time"),
        pl.col("trip_sec"),
        (pl.col("trip_sec").max() - pl.col("trip_sec").min()).alias("trip_sec_range"),
        (pl.col("trip_sec").std()).alias("trip_sec_std"),
        ((pl.col("trip_sec").max() / pl.col("trip_sec").min()) - 1).round(4).alias("max_trip_delay_ratio"),
        pl.col("monday").unique(),
        pl.col("tuesday").unique(),
        pl.col("wednesday").unique(),
        pl.col("thursday").unique(),
        pl.col("friday").unique(),
        pl.col("saturday").unique(),
        pl.col("sunday").unique(),
        pl.col("weekday").unique(),
        pl.col("weekend").unique(),
        pl.col("trip_id"),

    ).sort("shape_id")

    if not keep_cols:

        trips = trips.select(["route_id", "trip_headsign", "time_duration_bucket", "weekday", "weekend"]).unique()

    return trips

if __name__ == "__main__":
    # print(calc_route_time_diff(calc_trip_time(add_day_of_week(merge_routes_time())), keep_cols=False).explain())
    print(calc_route_time_diff(calc_trip_time(add_day_of_week(merge_routes_time()))).sink_ndjson("./time2.ndjson"))