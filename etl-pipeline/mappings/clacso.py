import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from uuid import uuid4
from typing import Optional
from model import Record, FRBRStatus, FileFlags, Part, Source
from .base_mapping import BaseMapping
from .rights import (
    get_rights_string,
    RIGHTS_STATEMENTS_TO_LICENSES,
    RIGHTS_LICENSES_TO_STATEMENTS,
)

HANDLE_URL = "biblioteca-repositorio.clacso.edu.ar/handle/CLACSO"
PDF_PART_URL = "https://biblioteca-repositorio.clacso.edu.ar"


class CLACSOMapping(BaseMapping):
    def __init__(self, clacso_record, namespaces):
        self.record = self._map_to_record(clacso_record, namespaces)

    def _map_to_record(self, record, namespaces) -> Record:
        clacso_record = record.xpath(".//oai_dc:dc", namespaces=namespaces)[0]

        medium = self._get_medium(clacso_record, namespaces)

        if medium is None:
            return None

        has_part = self._get_part(clacso_record, namespaces)
        if has_part is None:
            return None

        return Record(
            uuid=uuid4(),
            frbr_status=FRBRStatus.TODO.value,
            cluster_status=False,
            source=Source.CLACSO.value,
            source_id=self._get_source_id(clacso_record, namespaces),
            title=clacso_record.xpath("./dc:title/text()", namespaces=namespaces)[0],
            authors=self._get_authors(clacso_record, namespaces),
            medium=medium,
            publisher=self._get_publishers(clacso_record, namespaces),
            subjects=self._get_subjects(clacso_record, namespaces),
            languages=self._get_languages(clacso_record, namespaces),
            identifiers=self._get_identifiers(clacso_record, namespaces),
            rights=self._get_rights(clacso_record, namespaces),
            has_part=has_part,
            dates=self._get_dates(clacso_record, namespaces),
            date_created=datetime.now(timezone.utc).replace(tzinfo=None),
            date_modified=datetime.now(timezone.utc).replace(tzinfo=None),
        )

    def _get_authors(self, record, namespaces):
        return [
            f"{author}|||true"
            for author in record.xpath("./dc:creator/text()", namespaces=namespaces)
        ]

    def _get_publishers(self, record, namespaces):
        return [
            f"{publisher}||"
            for publisher in record.xpath(
                "./dc:publisher/text()", namespaces=namespaces
            )
        ]

    def _get_subjects(self, record, namespaces):
        return [
            f"{subject}||"
            for subject in record.xpath("./dc:subject/text()", namespaces=namespaces)
        ]

    def _get_languages(self, record, namespaces):
        return [
            f"||{language_code}"
            for language_code in record.xpath(
                "./dc:language/text()", namespaces=namespaces
            )
        ]

    def _get_rights(self, record, namespaces) -> Optional[str]:
        rights = [
            right for right in record.xpath("./dc:rights/text()", namespaces=namespaces)
        ]
        licenses = [right for right in rights if right.startswith("http")]
        rights_statements = [
            right
            for right in rights
            if not right.startswith("http") and not right.startswith("info:eu-repo")
        ]

        rights = list(
            set(
                [
                    get_rights_string(
                        rights_source=Source.CLACSO.value,
                        license=RIGHTS_STATEMENTS_TO_LICENSES.get(rights_statement),
                        rights_statement=rights_statement,
                    )
                    for rights_statement in rights_statements
                ]
                + [
                    get_rights_string(
                        rights_source=Source.CLACSO.value,
                        license=license,
                        rights_statement=RIGHTS_LICENSES_TO_STATEMENTS.get(license),
                    )
                    for license in licenses
                ]
            )
        )

        return next(filter(lambda right: right is not None, rights), None)

    def _get_source_id(self, record, namespaces):
        for identifier in record.xpath("./dc:identifier/text()", namespaces=namespaces):
            if HANDLE_URL in identifier:
                id = identifier.split("/")[-1]

                return f"{id}|{Source.CLACSO.value}"

    def _get_medium(self, record, namespaces):
        for medium in record.xpath("./dc:type/text()", namespaces=namespaces):
            medium_value = medium.split("/")[-1].lower()

            if (
                medium_value == "book"
                or medium_value == "libro"
                or medium_value == "livro"
            ):
                return medium_value

    def _get_identifiers(self, record, namespaces):
        return [
            self.format_identifier(id, "clacso")
            for id in record.xpath("./dc:identifier/text()", namespaces=namespaces)
        ]

    def _get_dates(self, record, namespaces):
        years_issued = [
            date.split("-")[0] if "-" in date else date
            for date in record.xpath("./dc:date/text()", namespaces=namespaces)
        ]

        return [f"{min(years_issued)}|publication_date"]

    def _get_part(self, record, namespaces):
        record_urls = [
            has_part
            for has_part in record.xpath(
                "./dc:identifier/text()", namespaces=namespaces
            )
            if "http" in has_part
        ]

        for record_url in record_urls:
            if HANDLE_URL in record_url or ("view" in record_url):
                links = self._get_links(url=record_url)

                for link in links:
                    if (
                        ".pdf" in link.get("href", [])
                        or "pdf" in link.get("class", [])
                        or "pdf" in link.text.lower()
                    ):
                        pdf_part = self._get_pdf_part(link=link)

                        if pdf_part:
                            return [str(pdf_part)]

    def _get_links(self, url: str) -> list:
        try:
            response = requests.get(url, timeout=10)
        except Exception:
            return []

        clacso_page = BeautifulSoup(response.text, "html.parser")

        return clacso_page.find_all("a")

    def _get_pdf_part(self, link: str) -> Optional[Part]:
        pdf_link = link.get("href")
        pdf_url = (
            f"{PDF_PART_URL}{pdf_link}" if "bitstream/CLACSO" in pdf_link else pdf_link
        )

        try:
            response = requests.head(pdf_url)
        except Exception:
            return None

        if response.status_code != 200:
            return None

        return Part(
            index=1,
            url=pdf_url,
            source=Source.CLACSO.value,
            file_type="application/pdf",
            flags=str(FileFlags(download=True)),
        )

    def createMapping(self):
        pass

    def applyFormatting(self):
        pass

    def applyMapping(self):
        pass
