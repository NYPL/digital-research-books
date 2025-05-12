class YearParser:
    def __init__(self, start, end):
        self.start = str(start)
        self.end = str(end)
        self.century = [None, None]
        self.decade = [None, None]
        self.year = [None, None]

    def setYearComponents(self):
        self.setCentury()
        self.setDecade()
        self.setYear()

    def setCentury(self):
        self.century[0] = int(self.start[:2])

        if len(self.end) > 2:
            self.century[1] = int(self.end[:2])

    def setDecade(self):
        if self.start[2] not in ["-", "?"]:
            self.decade[0] = int(self.start[2])

        if len(self.end) > 2 and self.end[2] not in ["-", "?"]:
            self.decade[1] = int(self.end[2])

    def setYear(self):
        if self.start[3] not in ["-", "?"]:
            self.year[0] = int(self.start[3])

        if len(self.end) > 2 and self.end[3] not in ["-", "?"]:
            self.year[1] = int(self.end[3])

    def __iter__(self):
        for key in ["century", "decade", "year"]:
            yearComp = getattr(self, key)

            if yearComp[0] is not None:
                yield "{}Start".format(key), yearComp[0]

            if yearComp[1] is not None:
                yield "{}End".format(key), yearComp[1]

    @staticmethod
    def convertYearDictToStr(yearDict):
        startYear = YearParser.getYearStr(yearDict, "Start")
        endYear = YearParser.getYearStr(yearDict, "End")

        return (
            "{}-{}".format(startYear, endYear)
            if endYear and endYear != startYear
            else str(startYear)
        )

    @staticmethod
    def getYearStr(yearDict, yearType):
        century = yearDict.get("century{}".format(yearType), None)
        decade = yearDict.get("decade{}".format(yearType), None)
        year = yearDict.get("year{}".format(yearType), None)

        return "".join(
            map(lambda x: str(x) if x is not None else "x", [century, decade, year])
        )
