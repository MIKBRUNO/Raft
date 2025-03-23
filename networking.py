from typing import Any
from abc import ABC, abstractmethod


class NetworkMember(ABC):
    @property
    @abstractmethod
    def id(self) -> Any: ...


    @abstractmethod
    def __eq__(self, other: 'NetworkMember') -> bool: ...


    @abstractmethod
    def __lt__(self, other) -> bool: ...


    @abstractmethod
    def __le__(self, other) -> bool: ...


class Network(ABC):
    @abstractmethod
    async def __init__(self, member: NetworkMember, other_members: list[NetworkMember]):
        super().__init__()
