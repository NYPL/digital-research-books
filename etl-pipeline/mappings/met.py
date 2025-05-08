from datetime import datetime, timezone
from uuid import uuid4

from model import FRBRStatus, FileFlags, Part, Record, Source
from .rights import get_rights_string


PDF_LINK = "https://libmma.contentdm.oclc.org/digital/api/collection/p15324coll10/id/{}/download"


def map_met_record(met_record: dict) -> Record:
    return Record(
        uuid=uuid4(),
        frbr_status=FRBRStatus.TODO.value,
        cluster_status=False,
        title=met_record.get("title"),
        alternative=[met_record.get("titlea")] if met_record.get("titlea") else None,
        source_id=f"{met_record.get('dmrecord')}|{Source.MET.value}",
        source=Source.MET.value,
        authors=[f"{met_record.get('creato')}|||true"]
        if met_record.get("creato")
        else None,
        languages=[f"{met_record.get('langua')}||"]
        if met_record.get("langua")
        else None,
        dates=[f"{met_record.get('date')}|publication_date"]
        if met_record.get("date")
        else None,
        publisher=[f"{met_record.get('publis')}"] if met_record.get("publis") else None,
        identifiers=[
            identifier
            for identifier in [
                f"{met_record.get('dmrecord')}|{Source.MET.value}"
                if met_record.get("dmrecord")
                else None,
                f"{met_record.get('identi')}|{Source.MET.value}"
                if met_record.get("identi")
                else None,
                f"{met_record.get('digiti')}|{Source.MET.value}"
                if met_record.get("digiti")
                else None,
                f"{met_record.get('dmcoclcno')}|oclc"
                if met_record.get("dmcoclcno")
                else None,
            ]
            if identifier
        ],
        contributors=[
            contributor
            for contributor in [
                f"{met_record.get('contri')}|||contributor"
                if met_record.get("contri")
                else None,
                f"{met_record.get('physic')}|||repository"
                if met_record.get("physic")
                else None,
                f"{met_record.get('source')}|||provider"
                if met_record.get("source")
                else None,
            ]
            if contributor
        ],
        extent=met_record.get("extent"),
        is_part_of=f"{met_record.get('relatig')}|collection"
        if met_record.get("relatig")
        else None,
        abstract=met_record.get("transc") or met_record.get("descri") or None,
        subjects=[
            f"{subject.strip()}||" for subject in met_record.get("subjec").split(";")
        ]
        if met_record.get("subjec")
        else None,
        rights=get_rights_string(
            rights_source=Source.MET.value,
            license=met_record.get("rights"),
            rights_reason=met_record.get("copyra"),
            rights_statement=met_record.get("copyri"),
        ),
        has_part=met_record.get("link")
        and [
            str(
                Part(
                    index=1,
                    url=met_record.get("link"),
                    source=Source.MET.value,
                    file_type="text/html",
                    flags=str(FileFlags(embed=True)),
                )
            ),
            str(
                Part(
                    index=1,
                    url=PDF_LINK.format(met_record.get("dmrecord")),
                    source=Source.MET.value,
                    file_type="application/pdf",
                    flags=str(FileFlags(download=True)),
                )
            ),
        ],
        date_created=datetime.now(timezone.utc).replace(tzinfo=None),
        date_modified=datetime.now(timezone.utc).replace(tzinfo=None),
    )
