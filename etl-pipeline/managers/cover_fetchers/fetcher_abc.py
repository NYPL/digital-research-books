from abc import ABC, abstractmethod 


class FetcherABC(ABC):
    @abstractmethod
    def __init__(self, *args):
        self.identifiers = args[0]
        self.cover_id = None

    @abstractmethod
    def has_cover(self):
        return False

    @abstractmethod
    def download_cover_file(self):
        return None
