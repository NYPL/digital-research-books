import re

from .year_parser import YearParser


def get_publication_date_object(dates: list[str]) -> dict:
    if dates is None or len(dates) < 1:
        return {}

    pubYears = {}

    for d in dates:
        try:
            date, dateType = tuple(d.split("|"))

            dateGroups = re.search(r"([\d\-\?]+)", date)

            dateStr = dateGroups.group(1)

            startYear, endYear = ("", "")

            if re.match(r"[0-9]{4}-[0-9]{4}", dateStr):
                rangeMatches = re.match(r"([0-9]{4})-([0-9]{4})", dateStr)
                startYear = rangeMatches.group(1)
                endYear = rangeMatches.group(2)
            elif re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", dateStr):
                year, _, _ = tuple(dateStr.split("-"))
                startYear, endYear = (year, year)
            elif re.match(r"[0-9]{4}-[0-9]{2}", dateStr):
                year, _ = tuple(dateStr.split("-"))
                startYear, endYear = (year, year)
            else:
                startYear, endYear = (dateStr, dateStr)

            yearParser = YearParser(startYear, endYear)
            yearParser.setYearComponents()
            pubYears[dateType] = dict(yearParser)
        except (ValueError, AttributeError, IndexError) as e:
            pass

    for datePref in ["copyright_date", "publication_date", "issued"]:
        if datePref in pubYears.keys():
            return pubYears[datePref]

    return {}
