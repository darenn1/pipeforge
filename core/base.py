from abc import ABC, abstractmethod
from typing import Any, Iterator


class Extractor(ABC):
 
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def extract(self) -> Iterator[Any]:
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


class Transformer(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def transform(self, records: Iterator[Any]) -> Iterator[Any]:
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


class Loader(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def load(self, records: Iterator[Any]) -> int:
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"