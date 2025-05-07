# Script run daily to scrape and initialize conversion for GRIN books acquired in the previous day
# Temporarily, script will also intialize conversion for backfilled books
from grin_client import GRINClient

class GRINConversion:
    def __init__(self):
        client = GRINClient()
        pass

    def acquire(self):
        # acquire the new books within the month
        pass

    def convert(self):
        # convert new books within the month
        pass

    def convert_backfills(self):
        # initialize conversion the new books, and update the DB
        pass

    def get_converted(self):
        # scrape the converted files from GRIN and update the DB
        pass