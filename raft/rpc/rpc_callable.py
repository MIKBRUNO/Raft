from abc import ABC, abstractmethod


class RPCCallable(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...


    @abstractmethod
    async def call[ArgsT](self, args: ArgsT) -> dict | None: ...
