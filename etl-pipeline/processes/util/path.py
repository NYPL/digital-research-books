import datetime
import pathlib
import re

COMBINED_OCR_PDF_DIR = pathlib.Path("combined_ocr_pdfs")
METADATA_DIR = pathlib.Path("metadata_files")
OCR_INPUT_DIR = pathlib.Path("ocr_inputs")
TAGGED_PDF_DIR = pathlib.Path("tagged_pdfs")
UNZIPPED_OCR_DIR = pathlib.Path("unzipped_ocr")


class METSPath:
    """Given an S3 key pointing to a METS file, provide path operations pointing
    to other useful S3 keys relative to the METS file"""

    def __init__(self, mets_key: str):
        self.mets_key = mets_key

    @classmethod
    def from_package_path(cls, ocr_package_name: str, mets_filename: str) -> "METSPath":
        return cls(OCR_INPUT_DIR / ocr_package_name / mets_filename)

    @property
    def _mets_path(self) -> pathlib.Path:
        return pathlib.Path(self.mets_key)

    @property
    def unzipped_ocr_target(self) -> pathlib.Path:
        """The folder where unzipped OCR data should go"""
        return UNZIPPED_OCR_DIR / self._mets_path.parent.name

    @property
    def combined_ocr_pdf_key(self) -> pathlib.Path:
        """The full key corresponding to the initial PDF with OCR text"""
        parent_path = COMBINED_OCR_PDF_DIR / self._mets_path.parent.name
        return _append_suffix(parent_path, ".pdf")

    @property
    def tagged_pdf_key(self) -> pathlib.Path:
        """The full key corresponding to the final tagged PDF"""
        parent_path = TAGGED_PDF_DIR / self._mets_path.parent.name
        return _append_suffix(parent_path, ".pdf")

    def get_metadata_key(self, mets_metadata_location: str | None) -> pathlib.Path:
        """Given a relative metadata filename, return its absolute path / key"""
        if not mets_metadata_location:
            return self.mets_key

        return UNZIPPED_OCR_DIR / self._mets_path.parent.name / mets_metadata_location

    def get_absolute_archive_key(
        self, mets_zip_file: str | None, mets_key: str
    ) -> pathlib.Path:
        """Given a relative archive filename, return its absolute path / key"""

        # GRIN packages are uploaded as tar.gz files
        if not mets_zip_file:
            return pathlib.Path(self.mets_key).parent / re.sub(
                r"(?:\.mets)?.xml", ".tar.gz", mets_key.split("/")[-1]
            )

        return pathlib.Path(self.mets_key).parent / mets_zip_file

    def get_metadata_file_key(self, date: datetime.date) -> pathlib.Path:
        """Get the key for the output metadata file for the given date"""
        formatted_date = date.strftime("%Y-%m-%d")
        parent_path = METADATA_DIR / formatted_date / self._mets_path.parent.name
        return _append_suffix(parent_path, ".json")


def _append_suffix(p: pathlib.Path, suffix: str) -> pathlib.Path:
    """Append a suffix to path, making sure not to replace an existing suffix
    in case the filename itself as a `.` in it"""
    return p.with_suffix(p.suffix + suffix)
