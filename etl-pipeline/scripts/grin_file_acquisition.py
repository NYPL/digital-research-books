from processes.grin.download import GRINDownload
import logging

logger = logging.getLogger()


def lambda_handler(event, context):
    # TODO: Add parsing of the actual StepFunction input to retrieve barcode correctly
    barcode = 33433115534525

    downloader = GRINDownload(barcode)
    downloader.run_process()


if __name__ == "__main__":
    lambda_handler()
