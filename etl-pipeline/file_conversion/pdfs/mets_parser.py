import dataclasses
import logging
import re
import typing

from lxml import etree

"""
METS Resources
- https://mets.github.io/METS_Docs/mets.html
- https://www.loc.gov/standards/mets/docs/mets.v1-6.html
"""

# https://github.com/ocrmypdf/OCRmyPDF/issues/453#issuecomment-761254016
DOCTYPE = """<!DOCTYPE html [<!ENTITY shy "&#173;"> <!ENTITY thinsp "&#2009;">]>"""

NSMAP = {
    "METS": "http://www.loc.gov/METS/",
    "xlink": "http://www.w3.org/1999/xlink",
    "PREMIS": "info:lc/xmlns/premis-v2",
}

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class FileDef:
    fid: str
    mime_type: str
    location: str
    use: str


@dataclasses.dataclass
class Page:
    id: str
    image_file: FileDef
    ocr_file: FileDef
    text_file: FileDef
    label: str | None
    order_label: str
    order: str

    @property
    def is_toc(self) -> bool:
        return "TABLE_OF_CONTENTS" in self._labels

    @property
    def is_chapter_start(self) -> bool:
        return "CHAPTER_START" in self._labels

    @property
    def _labels(self) -> list[str]:
        if not self.label:
            return []

        # HathiTrust labels - delimited by commas. GRIN labels - delimited by space.
        return [label.strip() for label in re.split(r"[,\s]+", self.label)]


class XMLParseError(ValueError):
    """A wrapper around the lxml provided error"""

    pass


class METSFile:
    def __init__(self, root):
        self.root = root

    @classmethod
    def from_mets_str(cls, mets_str: str) -> "METSFile":
        try:
            return cls(etree.fromstring(mets_str))
        except etree.XMLSyntaxError as e:
            logger.exception(e)
            raise XMLParseError()

    def iter_pages(self) -> typing.Iterator[Page]:
        struct_map = self._find("METS:structMap")
        file_mapping = self._map_file_ids()
        for page in struct_map.find(
            "METS:div",
            namespaces=NSMAP,
        ).findall("METS:div", namespaces=NSMAP):
            if not page.get("TYPE") == "page":
                continue

            yield self._parse_page(page, file_mapping)

    @property
    def page_count(self) -> int:
        struct_map = self._find("METS:structMap")
        page_divs = struct_map.find("METS:div", namespaces=NSMAP)

        return (
            len(
                [
                    div
                    for div in page_divs.findall("METS:div", namespaces=NSMAP)
                    if div.get("TYPE") == "page"
                ]
            )
            if struct_map is not None and page_divs is not None
            else 0
        )

    @property
    def zip_file(self) -> str | None:
        groups = self._find("METS:fileSec").findall("METS:fileGrp", namespaces=NSMAP)
        for group in groups:
            if group.get("USE") == "zip archive":
                return (
                    group.find("METS:file", namespaces=NSMAP)
                    .find("METS:FLocat", namespaces=NSMAP)
                    .xpath("@xlink:href", namespaces=NSMAP)[0]
                )

    @property
    def metadata_file(self) -> str | None:
        groups = self._find("METS:fileSec").findall("METS:fileGrp", namespaces=NSMAP)
        for group in groups:
            if group.get("USE") == "source METS":
                return (
                    group.find("METS:file", namespaces=NSMAP)
                    .find("METS:FLocat", namespaces=NSMAP)
                    .xpath("@xlink:href", namespaces=NSMAP)[0]
                )

    @property
    def first_page(self) -> str:
        return (
            self._find("METS:fileSec")
            .find("METS:fileGrp", namespaces=NSMAP)
            .find("METS:file", namespaces=NSMAP)
            .get("SEQ")
        )

    def _find(self, target: str):
        return self.root.find(target, namespaces=NSMAP)

    def _findall(self, target: str):
        return self.root.findall(target, namespaces=NSMAP)

    def _parse_page(self, page_elem, file_mapping) -> Page:
        image_file = next(
            file_mapping[fptr.get("FILEID")]
            for fptr in page_elem.findall("METS:fptr", namespaces=NSMAP)
            if file_mapping[fptr.get("FILEID")].use == "image"
        )
        ocr_file = next(
            file_mapping[fptr.get("FILEID")]
            for fptr in page_elem.findall("METS:fptr", namespaces=NSMAP)
            if file_mapping[fptr.get("FILEID")].use == "coordOCR"
        )
        text_file = next(
            file_mapping[fptr.get("FILEID")]
            for fptr in page_elem.findall("METS:fptr", namespaces=NSMAP)
            if file_mapping[fptr.get("FILEID")].use.lower() == "ocr"
        )
        id = text_file.fid.removeprefix("TXT")

        return Page(
            id=id,
            image_file=image_file,
            ocr_file=ocr_file,
            text_file=text_file,
            # HathiTrust uses LABEL. GRIN uses ADMID.
            label=page_elem.get("LABEL")
            if page_elem.get("LABEL")
            else page_elem.get("ADMID"),
            order_label=page_elem.get("ORDERLABEL"),
            order=page_elem.get("ORDER"),
        )

    def _map_file_ids(self) -> dict[str, FileDef]:
        id_map = {}
        file_sec = self._find("METS:fileSec")
        for group in file_sec.findall("METS:fileGrp", namespaces=NSMAP):
            use = group.get("USE")
            for file in group.findall("METS:file", namespaces=NSMAP):
                file_id = file.get("ID")
                location = file.find(
                    "METS:FLocat",
                    namespaces=NSMAP,
                ).xpath("@xlink:href", namespaces=NSMAP)[0]
                id_map[file_id] = FileDef(
                    fid=file_id,
                    mime_type=file.get("MIMETYPE"),
                    location=location,
                    use=use,
                )

        return id_map


@dataclasses.dataclass
class Metadata:
    md_type: str
    xml_data: etree._Element


@dataclasses.dataclass
class MetadataFileSourceIdentifier:
    source: str
    identifier_type: str
    identifier_value: str


class MetadataFile:
    """A metadata file in METS format"""

    def __init__(self, root):
        self.root = root

    @classmethod
    def from_mets_str(cls, mets_str: str) -> "MetadataFile":
        return cls(etree.fromstring(mets_str))

    def get_metadata(self) -> Metadata:
        md_wrapper = self.root.find("METS:dmdSec", namespaces=NSMAP).find(
            "METS:mdWrap", namespaces=NSMAP
        )
        return Metadata(
            md_type=md_wrapper.get("MDTYPE"),
            xml_data=md_wrapper.find("METS:xmlData", namespaces=NSMAP),
        )

    def get_source_identifier(self) -> MetadataFileSourceIdentifier:
        return MetadataFileSourceIdentifier(
            source=self.root.find(".//METS:name", namespaces=NSMAP).text,
            identifier_type=self.root.find(
                ".//PREMIS:objectIdentifierType", namespaces=NSMAP
            ).text,
            identifier_value=self.root.find(
                ".//PREMIS:objectIdentifierValue", namespaces=NSMAP
            ).text,
        )


def reset_hocr_doctype(ocr_file_location: str) -> None:
    parser = etree.HTMLParser()
    tree = etree.parse(ocr_file_location, parser=parser)
    ocr_string = etree.tostring(
        tree,
        pretty_print=True,
        xml_declaration=True,
        doctype=DOCTYPE,
    )
    # For some horrible reason, the `tostring` method does the following:
    #   - if xml_declaration is True and there is a DOCTYPE, it sandwiches the DOCTYPE
    #     between the existing declaration and the new one
    #   - if xml_declaration is False and there is a DOCTYPE, it keeps the existing
    #     declaration and places the doctype on top
    #   - and weirdest of all, if xml_declaration is False and there is no doctype set,
    #     it swaps the existing doctype and declaration, again rendering the document
    #     unparseable
    # So just do the first case and throw out the 3rd line (the original declaration)
    ocr_string = b"\n".join(
        line for i, line in enumerate(ocr_string.split(b"\n")) if i != 2
    )
    with open(ocr_file_location, "wb") as f:
        f.write(ocr_string)
