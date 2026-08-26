from zipfile import ZipFile


def extract_google_transit(file_loc = "./data/google_transit.zip", \
                           output_loc = "./data/gtfs/"):

    """
    specify paths with file_loc, output_loc.
    """
    
    with ZipFile(file_loc) as gtfs_zip:
        gtfs_zip.extractall(path=output_loc)