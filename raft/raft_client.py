from abc import ABC, abstractsmethod

from pydantic import BaseModel

from enum import Enum


class ResponseStatus(Enum):
    FIAL = 'FAIL'
    SUCCESS = 'SUCCESS'


class ApplyCommandResponse(BaseModel):
    status: ResponseStatus
    body: dict | None


class RaftClient(ABC):
    @abstractsmethod
    async def apply(self, command: dict) -> ApplyCommandResponse: ...
