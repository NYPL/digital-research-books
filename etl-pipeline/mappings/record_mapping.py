from abc import ABC, abstractmethod


class RecordMapping(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def createMapping(self):
        pass

    @abstractmethod
    def applyFormatting(self):
        pass

    @abstractmethod
    def applyMapping(self):
        pass
