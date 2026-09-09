""" extracts GTFS data from ./data/google_transit.zip
see your local agency's website for accessing their GTFS-static data.
"""

from sdmts_traffic import prep_gtfs

prep_gtfs.extract_google_transit()