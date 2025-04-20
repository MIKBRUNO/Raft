from abc import ABC, abstractmethod


class RPCCallable(ABC):
    @abstractmethod
    async def call(self, args: dict) -> dict | None:
        ...
