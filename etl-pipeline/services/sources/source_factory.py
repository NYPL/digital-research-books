from typing import Optional
from mappings.clacso import CLACSOMapping
from mappings.doab import DOABMapping
from model import Source
from .source_service import SourceService
from .chicago_isac_service import ChicagoISACService
from .dspace_service import DSpaceService
from .gutenberg_service import GutenbergService
from .hathi_trust_service import HathiTrustService
from .met_service import METService
from .muse_service import MUSEService
from .nypl_bib_service import NYPLBibService
from .publisher_backlist_service import PublisherBacklistService

CLACSO_BASE_URL = "https://biblioteca-repositorio.clacso.edu.ar/oai/request?"
DOAB_BASE_URL = "https://directory.doabooks.org/oai/request?"
DOAB_IDENTIFIER = "oai:directory.doabooks.org"


def get_source_service(source: str) -> Optional[SourceService]:
    if source == Source.CHICACO_ISAC.value:
        return ChicagoISACService()
    if source == Source.CLACSO.value:
        return DSpaceService(base_url=CLACSO_BASE_URL, source_mapping=CLACSOMapping)
    if source == Source.DOAB.value:
        return DSpaceService(
            base_url=DOAB_BASE_URL,
            source_identifier=DOAB_IDENTIFIER,
            source_mapping=DOABMapping,
        )
    if source == Source.GUTENBERG.value:
        return GutenbergService()
    if source == Source.HATHI.value:
        return HathiTrustService()
    if source == Source.MET.value:
        return METService()
    if source == Source.MUSE.value:
        return MUSEService()
    if source == Source.NYPL.value:
        return NYPLBibService()
    if source == Source.PUBLISHER_BACKLIST.value:
        return PublisherBacklistService()

    raise Exception(f"No service found for source: {source}")
