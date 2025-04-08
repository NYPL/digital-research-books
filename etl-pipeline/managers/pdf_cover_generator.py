import io
import os

import PIL
import pypdf
import requests
import requests.exceptions

from logger import create_log

logger = create_log(__name__)


class PDFCoverGenerator:

    def __init__(self, pdf_content: io.BytesIO):

        self.pdf = pypdf.PdfReader(pdf_content)

    @staticmethod
    def from_url(pdf_url: str) -> "PDFCoverGenerator":
        stream = io.BytesIO()
        logger.info("Fetching PDF from url %s", pdf_url)
        response = requests.get(pdf_url, stream=True)
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=8192):
            stream.write(chunk)

        return PDFCoverGenerator(stream)

    def extract_cover_content(self, outstream: io.BytesIO) -> None:
        image = self._extract_cover_image()
        image.save(outstream, format="PNG")
        # Move the stream pointer back to the front now that we're done writing
        outstream.seek(0)

    def _extract_cover_image(self) -> PIL.Image:
        for page in self.pdf.pages:
            image = page.images[0].image
            if page.extract_text():
                logger.info("Found first page with text, returning as cover")
                return image

            if image.entropy() < 0.001:
                logger.info("Likely blank image detected, skipping")
                continue

            return image

        # Default to the first page
        return self.pdf.pages[0]
