from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseExchangeAdapter(ABC):
    slug: str

    @abstractmethod
    def fetch_all_balances(self) -> Dict[str, Any]: ...
    @abstractmethod
    def fetch_open_positions(self) -> List[Dict[str, Any]]: ...
    @abstractmethod
    def fetch_funding(self, limit: int = 50) -> List[Dict[str, Any]]: ...
    @abstractmethod
    def save_closed_positions(self, db_path: str) -> None: ...

