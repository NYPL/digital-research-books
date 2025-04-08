import inspect
from io import BytesIO
from PIL import Image, UnidentifiedImageError

import managers.cover_fetchers as fetchers


class CoverManager:
    def __init__(self, identifiers, db_session):
        self.identifiers = identifiers
        self.db_session = db_session

        self.load_fetchers()

        self.fetcher = None
        self.cover_content = None
        self.cover_format = None

    def load_fetchers(self):
        fetcher_list = inspect.getmembers(fetchers, inspect.isclass)

        self.fetchers = [None] * len(fetcher_list)

        for fetcher in fetcher_list:
            _, FetcherClass = fetcher
            self.fetchers[FetcherClass.ORDER - 1] = FetcherClass

    def fetch_cover(self):
        for fetcher in self.fetchers:
            fetcher = fetcher(self.identifiers, self.db_session)
            
            if fetcher.has_cover() is True:
                self.fetcher = fetcher
                return True

        return False

    def fetch_cover_file(self):
        self.cover_content = self.fetcher.download_cover_file()

    def resize_cover_file(self):
        try:
            original = Image.open(BytesIO(self.cover_content))
        except UnidentifiedImageError:
            self.cover_content = None
            return None

        original_ratio = original.width / original.height

        resize_height = 400
        resize_width = 300

        if 400 * original_ratio > 300 and original_ratio > 1:
            resize_height = int(round(300 / original_ratio))
        elif 400 * original_ratio > 300:
            resize_height = int(round(300 * original_ratio))
        else:
            resize_width = int(round(400 * original_ratio))

        resized = original.resize((resize_width, resize_height))

        resized_content = BytesIO()
        resized.save(resized_content, format=original.format)

        self.cover_content = resized_content.getvalue()
        self.cover_format = original.format
