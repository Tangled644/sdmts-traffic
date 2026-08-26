import geopandas as gpd

from sdmts_traffic import utils

shapes = gpd.read_file(utils.shapes_geojson(r"2\d\d_", test=False, regex=True))
map = shapes.explore(tiles="CartoDB Positron")
map.save("sdmts.html")
print(f"compiled {len(shapes)} shape(s) into map")