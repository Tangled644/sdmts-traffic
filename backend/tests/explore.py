import matplotlib.pyplot as plt
import polars as pl

import sdmts_traffic.trip_analysis as ta

lf = ta.run_analysis(drop_dow=True)
lf = lf.filter(pl.col("shape_id") != "null")
lf = lf.sort("max_trip_delay_ratio", descending=True).select(["route_id", "max_trip_delay_ratio"])
lf = lf.unique(subset="route_id", keep="first").sort("max_trip_delay_ratio", descending=True)

df = lf.head(10).collect()
routes = df.select("route_id").to_series().to_list()
delay = df.select("max_trip_delay_ratio").to_series().to_list()


fig, ax = plt.subplots()

ax.barh(routes, delay)
ax.set_yinverted(True)
ax.set_xbound(1, 2)

plt.show()