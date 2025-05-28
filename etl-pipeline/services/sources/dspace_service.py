from datetime import datetime
from typing import Optional, Generator
import requests
from io import BytesIO
from lxml import etree

from constants.get_constants import get_constants
from logger import create_log
from mappings.base_mapping import MappingError
from mappings.xml import XMLMapping
from model import Record
from .source_service import SourceService

logger = create_log(__name__)


class DSpaceService(SourceService):
    ROOT_NAMESPACE = {None: "http://www.openarchives.org/OAI/2.0/"}
    OAI_NAMESPACES = {
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "datacite": "https://schema.datacite.org/meta/kernel-4.1/metadata.xsd",
        "oapen": "http://purl.org/dc/elements/1.1/",
        "oaire": "https://raw.githubusercontent.com/rcic/openaire4/master/schemas/4.0/oaire.xsd",
    }

    def __init__(
        self, base_url, source_mapping: type[XMLMapping], source_identifier: str = None
    ):
        self.constants = get_constants()

        self.base_url = base_url
        self.source_mapping = source_mapping
        self.source_identifier = source_identifier

    def get_records(
        self,
        start_timestamp: Optional[datetime] = None,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Generator[Record, None, None]:
        resumption_token = None
        record_index = 0
        record_count = 0

        while resumption_token is not None or record_index <= offset:
            oai_file = self.download_records(
                start_timestamp=start_timestamp, resumption_token=resumption_token
            )
            resumption_token = self.get_resumption_token(oai_file)

            if offset is not None and record_index <= offset:
                record_index += 100
                continue

            oaidc_records = etree.parse(oai_file)

            all_records = oaidc_records.findall(
                ".//record", namespaces=self.ROOT_NAMESPACE
            )

            for record in all_records:
                if record is None:
                    continue

                try:
                    parsed_record = self.parse_record(record)

                    if parsed_record.record is None:
                        continue

                    yield parsed_record.record
                    record_count += 1

                    if limit is not None and record_count >= limit:
                        return
                except Exception:
                    logger.error(f"Error parsing DSpace record {record}")

    def parse_record(self, record):
        try:
            parsed_record = self.source_mapping(record, self.OAI_NAMESPACES)
            return parsed_record
        except MappingError as e:
            raise Exception(e.message)

    def get_record(self, record_id: str):
        url = f"{self.base_url}verb=GetRecord&metadataPrefix=oai_dc&identifier={self.source_identifier}:{record_id}"

        # Warning: bypassing verification of SSL certs is a security concern. However, some sources do not keep their DSpace SSL cert up to date.
        response = requests.get(url, timeout=30, verify=False)

        if response.status_code == 200:
            content = BytesIO(response.content)
            oaidc_XML = etree.parse(content)
            oaidc_record = oaidc_XML.find("//record", namespaces=self.ROOT_NAMESPACE)

            try:
                parsed_record = self.parse_record(oaidc_record)
                return parsed_record
            except Exception as e:
                logger.error(f"Error parsing DSpace record {oaidc_record}")

    def get_resumption_token(self, oai_file):
        try:
            oai_XML = etree.parse(oai_file)
            return oai_XML.find(
                ".//resumptionToken", namespaces=self.ROOT_NAMESPACE
            ).text
        except AttributeError:
            return None

    def download_records(
        self, start_timestamp: Optional[datetime], resumption_token=None
    ):
        headers = {
            # Pass a user-agent header to prevent 403 unauthorized responses from DSpace
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        url_params = "verb=ListRecords"
        if resumption_token:
            url_params += f"&resumptionToken={resumption_token}"
        elif start_timestamp:
            url_params += (
                f"&metadataPrefix=oai_dc&from={start_timestamp.strftime('%Y-%m-%d')}"
            )
        else:
            url_params += f"&metadataPrefix=oai_dc"

        url = f"{self.base_url}{url_params}"

        # Warning: bypassing verification of SSL certs is a security concern. However, some sources do not keep their DSpace SSL cert up to date.
        response = requests.get(url, stream=True, timeout=30, headers=headers, verify=False)

        if response.status_code == 200:
            content = bytes()

            for chunk in response.iter_content(1024 * 100):
                content += chunk

            return BytesIO(content)

        raise Exception(f"Received {response.status_code} status code from {url}")
