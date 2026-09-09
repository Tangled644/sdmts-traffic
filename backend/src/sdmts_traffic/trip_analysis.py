import polars as pl

from sdmts_traffic import missingGTFS


def trip_start_end(dir:str = "./data/gtfs/") -> pl.LazyFrame:
    """Extracts start and end time for trips in trips.txt.

    Parameters
    -------
    dir : str, optional
        GTFS directory.

    Returns
    -------
    pl.LazyFrame
        Includes start and end time.
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

def merge_routes_time(dir:str = "./data/gtfs/") -> pl.LazyFrame:
    """Uses shape_id of trips.txt to merge trips.

    Parameters
    -------
    dir : str, optional
        GTFS directory.

    Returns
    -------
    pl.LazyFrame
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

def add_day_of_week(lf:pl.LazyFrame, dir:str = "./data/gtfs/") -> pl.LazyFrame:
    """Adds a column indicating days of week the trip_id operates.
    Merges trips.txt and calendar.txt on service_id.

    Parameters
    -------
    lf : pl.LazyFrame
        input from merge_routes_time()
    dir : str, optional
        GTFS directory.

    Returns
    -------
    _type_
        _description_
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

    # compose weekday/weekend cols.
    schedule = schedule.with_columns([
        pl.all_horizontal("monday", "tuesday", "wednesday", "thursday", "friday").alias("weekday"),
        pl.any_horizontal("saturday", "sunday").alias("weekend")
    ])
    schedule = lf.join(schedule, on="service_id").with_columns((pl.col("end_date").str.to_date(r"%Y%m%d") - pl.col("start_date").str.to_date(r"%Y%m%d")).alias("date_range"))

    return schedule.sort(["shape_id", "start_time"])
    

def calc_trip_time(lf:pl.LazyFrame, time_bucket:str="15m", start_time:str="start_time", end_time:str="end_time") -> pl.LazyFrame:
    """Calculates trip time based on start and end time of input df.
    Assumes output comes from merge_routes_time() or add_day_of_week().
    Time columns should be of str, "HH:MM:SS", HH>23 ok (gtfs standard).
    Adds trip_sec, trip_time, buckets for sorting.

    Parameters
    ----------
    lf : pl.LazyFrame
        From merge_routes_time()
    time_bucket : str, optional
        Output time buckets for histogram graphing, by default "15m"
    start_time : str, optional
        start time column name, by default "start_time"
    end_time : str, optional
        end time column name, by default "end_time"

    Returns
    -------
    pl.LazyFrame
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


def calc_route_time_diff(lf:pl.LazyFrame, preserve_dow:bool=True,keep_holiday:bool=False, keep_cols:bool = True) -> pl.LazyFrame:
    """Calculates travel time differences among the same shape_id, service id.
    shape_id is used to maintain vehicle distance across identical headsigns.
    service_id is kept for day-of-week specific information.
    time_duration_bucket is a struct with start and end buckets, as well as trip time for that unique trip.

    if preserve_dow = False, time_duration_bucket will likely have many results with identical times. This is not deduplicated.
    

    Parameters
    ----------
    lf : pl.LazyFrame
        From calc_trip_time()
    preserve_dow : bool, optional
        Keep day of week information, by default True
    keep_holiday : bool, optional
        Keep trips that do not run the for the entire GTFS duration, such as holidays, by default False
    keep_cols : bool, optional
        Keep all output columns, by default True

    Returns
    -------
    pl.LazyFrame
    """

    if keep_holiday:
        trips = lf
    else:
    # exclude temporary routes, such as weekend routes for weekday holidays
    # may exclude starts and ends of service that do not coincide with service changes
    # ex: school tripper service
        trips = lf.filter(pl.col("date_range").dt.total_days() > 28)

    if preserve_dow:
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
            (pl.col("trip_sec").std()).round(4).alias("trip_sec_std"),
            (pl.col("trip_sec").max() / pl.col("trip_sec").min()).round(4).alias("max_trip_delay_ratio"),
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
    else:
        trips = trips.group_by("shape_id").agg(
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
            (pl.col("trip_sec").std()).round(4).alias("trip_sec_std"),
            (pl.col("trip_sec").max() / pl.col("trip_sec").min()).round(4).alias("max_trip_delay_ratio"),
            pl.col("trip_id"),
            pl.col("service_id")

        ).sort("shape_id")


    if not keep_cols:

        trips = trips.select(["route_id", "trip_headsign", "time_duration_bucket", "weekday", "weekend"]).unique()

    return trips


def run_analysis(drop_dow=False) -> pl.LazyFrame:
    """Runs default analysis functions in trip_analysis.py above.

    Parameters
    -------
    drop_dow : bool, optional
        Passed to calc_route_time_diff to group by only shape_id, not service_id, by default False
    Returns
    -------
    pl.LazyFrame
        Trips grouped by shape_id, service_id, with time information. Useful for further visualizaiton.
    """

    try:
        add_day_of_week(merge_routes_time()).collect_schema()
    except FileNotFoundError:
        raise missingGTFS("Ensure GTFS has been extracted, see extract_gtfs.py for usage.")

    return calc_route_time_diff(calc_trip_time(add_day_of_week(merge_routes_time())), preserve_dow=(not drop_dow))
