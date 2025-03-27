from typing import Any, Callable, Awaitable
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
    def __init__(self, member: NetworkMember, other_members: list[NetworkMember]):
        super().__init__()


    @abstractmethod
    def get_connected_members(self) -> list[NetworkMember]: ...


    @abstractmethod
    def set_disconnected_callback(self, cb: Callable[[NetworkMember], Awaitable] | None) -> None: ...


    @abstractmethod
    def set_read_callback(self, cb: Callable[[NetworkMember, bytes], Awaitable] | None) -> None: ...
    
    
    @abstractmethod
    def set_connected_callback(self, cb: Callable[[NetworkMember], Awaitable] | None) -> None: ...


    @abstractmethod
    async def send(self, member: NetworkMember, msg: bytes) -> None: ...


    @abstractmethod
    async def close(self) -> None: ...
