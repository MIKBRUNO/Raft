from typing import Any, Callable, Awaitable
from abc import ABC, abstractmethod


class NetworkMember(ABC):
    @property
    @abstractmethod
    def id(self) -> Any: ...


class Network(ABC):
    @abstractmethod
    def __init__(self, member: NetworkMember, other_members: list[NetworkMember]):
        super().__init__()


    @property
    @abstractmethod
    def connected_members(self) -> list[NetworkMember]: ...


    @property
    @abstractmethod
    def members(self) -> list[NetworkMember]: ...


    @property
    @abstractmethod
    def this(self) -> NetworkMember: ...


    @abstractmethod
    async def run(self): ...


    @abstractmethod
    def set_disconnected_callback(self, cb: Callable[[NetworkMember], None | Awaitable[None]] | None) -> None: ...


    @abstractmethod
    def set_read_callback(self, cb: Callable[[NetworkMember, bytes], None | Awaitable[None]] | None) -> None: ...
    
    
    @abstractmethod
    def set_connected_callback(self, cb: Callable[[NetworkMember], None | Awaitable[None]] | None) -> None: ...


    @abstractmethod
    async def send(self, member: NetworkMember, msg: bytes) -> None: ...
