from abc import ABC, abstractsmethod


class RaftClient(ABC):
    @abstractsmethod
    async def apply(self, command: dict) -> dict: ...
