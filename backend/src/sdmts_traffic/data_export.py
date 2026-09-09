import polars as pl

import sdmts_traffic.trip_analysis as ta


# wip

def write_default():
    lf = ta.run_analysis()
    lf.sink_ndjson("./data/analysis/time2.ndjson")

def write_by_route(*routes:str) -> dict:
    lf = ta.run_analysis(drop_dow=True)
    print(lf.sink_ndjson("./data/analysis/time3.ndjson"))

    return {}


if __name__ == "__main__":
    write_default()
    write_by_route()