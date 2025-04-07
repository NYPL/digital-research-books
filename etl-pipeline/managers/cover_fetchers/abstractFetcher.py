from abc import ABC, abstractmethod 


class AbstractFetcher(ABC):
    @abstractmethod
    def __init__(self, *args):
        self.identifiers = args[0]
        self.coverID = None

    @abstractmethod
    def hasCover(self):
        return False

    @abstractmethod
    def downloadCoverFile(self):
        return None
