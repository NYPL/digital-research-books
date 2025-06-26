import dataclasses

from lxml import etree


NSMAP = {None: "http://www.loc.gov/MARC21/slim"}


@dataclasses.dataclass
class Subfield:
    code: str
    content: str

    @classmethod
    def from_node(cls, node: etree._Element):
        return cls(node.get("code"), node.text)


@dataclasses.dataclass
class DataField:
    tag: str
    ind1: str
    ind2: str
    subfields: list[Subfield]

    @classmethod
    def from_node(cls, node: etree._Element):
        return cls(
            tag=node.get("tag"),
            ind1=node.get("ind1"),
            ind2=node.get("ind2"),
            subfields=[
                Subfield.from_node(sf)
                for sf in node.findall("subfield", namespaces=NSMAP)
            ],
        )

    def subfield_by_code(self, code: str) -> Subfield | None:
        return next((sf for sf in self.subfields if sf.code == code), None)


@dataclasses.dataclass
class ControlField:
    tag: str
    content: str

    @classmethod
    def from_node(cls, node: etree._Element):
        return cls(tag=node.get("tag"), content=node.text)


@dataclasses.dataclass
class Record:
    controlfields: list[ControlField]
    datafields: list[DataField]

    @classmethod
    def from_node(cls, node: etree._Element):
        if node.find("record", namespaces=NSMAP) is not None:
            node = node.find("record", namespaces=NSMAP)

        return cls(
            controlfields=[
                ControlField.from_node(child)
                for child in node.findall("controlfield", namespaces=NSMAP)
            ],
            datafields=[
                DataField.from_node(child)
                for child in node.findall("datafield", namespaces=NSMAP)
            ],
        )

    @property
    def oclc_number(self) -> str | None:
        oclc_number_control_field = self._control_field_by_tag("001")

        if not oclc_number_control_field:
            return None

        return oclc_number_control_field.content.strip()

    @property
    def isbn(self) -> str | None:
        isbn_control_field = self._control_field_by_tag("020")

        if not isbn_control_field:
            return None

        return isbn_control_field.content.strip()

    @property
    def author(self) -> str | None:
        author_field = self._df_by_tag("100")
        if not author_field:
            return None
        return " ".join(subfield.content for subfield in author_field.subfields)

    @property
    def publication_date(self) -> str | None:
        publisher_field = self._df_by_tag("260")
        if not publisher_field:
            return None

        subfield = publisher_field.subfield_by_code("c")
        return subfield.content if subfield else None

    @property
    def publication_place(self) -> str | None:
        publisher_field = self._df_by_tag("260")
        if not publisher_field:
            return None

        subfield = publisher_field.subfield_by_code("a")
        return subfield.content if subfield else None

    @property
    def publisher(self) -> str | None:
        publisher_field = self._df_by_tag("260")
        if not publisher_field:
            return None

        subfield = publisher_field.subfield_by_code("b")
        return subfield.content if subfield else None

    @property
    def subject(self) -> str | None:
        subject_field = self._df_by_tag("650")
        if not subject_field:
            return None

        subfield = subject_field.subfield_by_code("a")
        return subfield.content if subfield else None

    @property
    def title(self) -> str | None:
        title_field = self._df_by_tag("245")
        if not title_field:
            return None

        return " ".join(subfield.content for subfield in title_field.subfields)

    @property
    def _publisher_field(self) -> DataField | None:
        return self._df_by_tag("260")

    def _control_field_by_tag(self, tag: str) -> ControlField | None:
        return next((cf for cf in self.controlfields if cf.tag == tag), None)

    def _df_by_tag(self, tag: str) -> DataField | None:
        return next((df for df in self.datafields if df.tag == tag), None)
