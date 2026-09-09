import sdmts_traffic.trip_analysis as ta

print(ta.run_analysis().sink_ndjson("./data/analysis/time.ndjson"))

