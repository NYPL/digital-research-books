import boto3
from ..processes.grin.download import GRINDownload
from managers import DBManager, S3Manager

s3_manager = S3Manager()
logger = logging.getLogger()

def lambda_handler(event, context):
    # TODO: Add parsing of the actual StepFunction input to retrieve barcode correctly
    barcode = event

    # Download TAR file from GRIN
    downloader = GRINDownload()
    downloader.download_book(barcode)
    
    # Upload TAR file to Glacier

    # Unpack TAR file

    # Upload unpacked files to s3 standard