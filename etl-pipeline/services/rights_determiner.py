import requests
from constants.get_constants import get_constants
from mappings.rights import get_rights_string
from model import Source

hathi_catalog_url = "https://catalog.hathitrust.org/api/volumes/brief/htid/{}.json"
hathi_constansts = get_constants()["hathitrust"]


def determine_rights(barcode) -> str | None:
    htid = f"nyp.{barcode}"
    catalog_response = requests.get(hathi_catalog_url.format(htid))

    if catalog_response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"HathiTrust API request failed with status code {catalog_response.status_code}"
        )

    catalog_data = catalog_response.json()

    for item in catalog_data.get("items", []):
        if item.get("htid") == htid:
            rights_code = item.get("rightsCode")
            rights = hathi_constansts["rightsValues"].get(
                rights_code, {"license": "und", "statement": "Copyright undetermined"}
            )

            return get_rights_string(
                rights_source=Source.HATHI.value,
                license=rights["license"],
                rights_statement=rights["statement"],
            )

    raise ValueError(
        f"No matching record found for barcode {barcode} in HathiTrust catalog"
    )
