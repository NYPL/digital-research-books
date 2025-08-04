import re


class TextCleaner:
    def __init__(self, text: str):
        self.text = text

    def remove_non_printable_characters(self):
        self.text = re.sub(r"[^ -~\n]+", "", self.text)
        return self

    def remove_redudant_newlines(self):
        self.text = re.sub(r"\n{2,}", "\n\n", self.text)
        return self

    def strip(self):
        self.text = self.text.strip()
        return self
