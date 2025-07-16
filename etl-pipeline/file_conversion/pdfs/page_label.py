import dataclasses
import string
import re

from pypdf.constants import PageLabelStyle
import roman

PAGE_LABEL_STYLE_UNKNOWN = "UNKNOWN"


@dataclasses.dataclass
class PageLabelRange:
    start_idx: int
    end_idx: int
    start_label: str
    style: str

    @property
    def start_label_num(self) -> int:
        return _label_to_num(self.start_label, self.style)

    @property
    def end_label_num(self) -> int:
        return self.start_label_num + self.end_idx - self.start_idx


def _label_to_num(label: str, style: PageLabelStyle) -> int:
    match style:
        case PageLabelStyle.DECIMAL:
            return int(label)
        case PageLabelStyle.LOWERCASE_ROMAN | PageLabelStyle.UPPERCASE_ROMAN:
            return roman.fromRoman(label.upper())
        case PageLabelStyle.LOWERCASE_LETTER | PageLabelStyle.UPPERCASE_LETTER:
            return ord(label) % 32
        case _:
            return 1


def _num_to_label(num: int, style: PageLabelStyle) -> str:
    match style:
        case PageLabelStyle.DECIMAL:
            return str(num)
        case PageLabelStyle.LOWERCASE_ROMAN:
            return roman.toRoman(num).lower()
        case PageLabelStyle.UPPERCASE_ROMAN:
            return roman.toRoman(num)
        case PageLabelStyle.LOWERCASE_LETTER:
            return string.ascii_lowercase[num - 1]
        case PageLabelStyle.UPPERCASE_LETTER:
            return string.ascii_uppercase[num - 1]
        case _:
            return 1


class PageLabeler:
    def __init__(self):
        self.ranges: list[PageLabelRange] = []

    def add_page_label(self, page_index: int, label: str) -> None:
        if not self.ranges:
            self._start_new_range(page_index, label)
            return

        latest_range = self.ranges[-1]
        if (
            _num_to_label(latest_range.end_label_num + 1, latest_range.style) == label
            and latest_range.end_idx + 1 == page_index
        ):
            latest_range.end_idx = page_index
        else:
            self._start_new_range(page_index, label)

    def write(self, writer) -> None:
        for page_label_range in self.ranges:
            writer.set_page_label(
                page_label_range.start_idx,
                page_label_range.end_idx,
                style=page_label_range.style,
                start=page_label_range.start_label_num,
            )

    def _start_new_range(self, page_index: int, start_label: str) -> None:
        self.ranges.append(
            PageLabelRange(
                start_idx=page_index,
                end_idx=page_index,
                start_label=start_label,
                style=self._infer_style(start_label),
            ),
        )

    def _infer_style(self, label: str) -> str:
        try:
            int(label)
            return PageLabelStyle.DECIMAL
        except ValueError:
            pass

        try:
            roman.fromRoman(label.upper())

            if label.isupper():
                return PageLabelStyle.UPPERCASE_ROMAN
            else:
                return PageLabelStyle.LOWERCASE_ROMAN
        except roman.InvalidRomanNumeralError:
            pass

        if re.fullmatch(r"[a-z]", label):
            return PageLabelStyle.LOWERCASE_LETTER
        elif re.fullmatch(r"[A-Z]", label):
            return PageLabelStyle.UPPERCASE_LETTER

        return PAGE_LABEL_STYLE_UNKNOWN
