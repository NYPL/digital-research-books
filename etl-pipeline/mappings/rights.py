from datetime import datetime
from typing import Optional


RIGHTS_STATEMENTS_TO_LICENSES = {
    "Attribution 4.0 International": "https://creativecommons.org/licenses/by/4.0/",
    "Attribution-ShareAlike 4.0 International": "https://creativecommons.org/licenses/by-sa/4.0/",
    "Attribution-NoDerivatives 4.0 International": "https://creativecommons.org/licenses/by-nd/4.0/",
    "Attribution-NonCommercial 4.0 International": "https://creativecommons.org/licenses/by-nc/4.0/",
    "Attribution-NonCommercial-ShareAlike 4.0 International": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    "Attribution-NonCommercial-NoDerivatives 4.0 International": "https://creativecommons.org/licenses/by-nc-nd/4.0/",
    "Attribution 3.0 Unported": "https://creativecommons.org/licenses/by/3.0/",
    "Attribution-ShareAlike 3.0 Unported": "https://creativecommons.org/licenses/by-sa/3.0/",
    "Attribution-NoDerivatives 3.0 Unported": "https://creativecommons.org/licenses/by-nd/3.0/",
    "Attribution-NonCommercial 3.0 Unported": "https://creativecommons.org/licenses/by-nc/3.0/",
    "Attribution-NonCommercial-ShareAlike 3.0 Unported": "https://creativecommons.org/licenses/by-nc-sa/3.0/",
    "Attribution-NonCommercial-NoDerivatives 3.0 Unported": "https://creativecommons.org/licenses/by-nc-nd/3.0/",
    "Attribution-NonCommercial-NoDerivs 3.0 Unported": "https://creativecommons.org/licenses/by-nc-nd/3.0/",
}

RIGHTS_LICENSES_TO_STATEMENTS = {
    license: rights_statement
    for rights_statement, license in RIGHTS_STATEMENTS_TO_LICENSES.items()
}


def get_rights_string(
    rights_source: str,
    license: Optional[str] = None,
    rights_reason: Optional[str] = None,
    rights_statement: Optional[str] = None,
    rights_date: Optional[datetime] = None,
) -> Optional[str]:
    if not license:
        return None

    return f"{rights_source}|{license or ''}|{rights_reason or ''}|{rights_statement or ''}|{rights_date or ''}"
