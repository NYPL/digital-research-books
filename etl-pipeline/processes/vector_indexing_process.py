import os

from logger import create_log
from vector_indexing.pipeline.orchestrator import main

logger = create_log(__name__)


class VectorIndexingProcess:
    def __init__(self, *args):
        self.barcodes = args[-1]  # options is the last argument

    def run(self):
        print(self.barcodes)
        main(barcodes=self.barcodes)
