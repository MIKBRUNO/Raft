from enum import Enum
from pydantic import BaseModel, ConfigDict, ValidationError
from typing import Any

import logging

from raft import RaftSM, RaftObject, RaftLogItem


__logger__ = logging.getLogger(__name__)


class CommandType(Enum):
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'


class DictCommand(BaseModel):
    model_config = ConfigDict(extra='allow')
    cmd_type: CommandType


class UpdateCommand(DictCommand):
    cmd_type: CommandType = CommandType.UPDATE
    key: str
    value: Any


class DeleteCommand(DictCommand):
    cmd_type: CommandType = CommandType.DELETE
    key: str


class Dictionary:
    class _DictSm(RaftSM):
        def __init__(self, this: 'Dictionary'):
            super().__init__()
            self._this: 'Dictionary' = this


        def apply_commands(self, commands: list[RaftLogItem]):
            for item in commands:
                try:
                    dcmd = DictCommand.model_validate(item.command)
                    if dcmd.cmd_type == CommandType.UPDATE:
                        cmd = UpdateCommand.model_validate(item.command)
                        self._this._dict[cmd.key] = cmd.value
                    elif dcmd.cmd_type == CommandType.DELETE:
                        cmd = DeleteCommand.model_validate(item.command)
                        self._this._dict.pop(cmd.key, None)
                except ValidationError as e:
                    __logger__.error(e)

    
    def __init__(self, raft: RaftObject):
        self._dict = {}
        self._raft = raft
        raft.set_raft_sm(self._DictSm(self))


    async def update(self, key: str, value: Any) -> Any:
        await self._raft.add_log_item(UpdateCommand(key=key, value=value).model_dump())
        return self._dict.get(key)

    
    async def delete(self, key: str) -> Any:
        await self._raft.add_log_item(DeleteCommand(key=key).model_dump())
        return self._dict.get(key)
    

    def get(self, key: str) -> Any:
        return self._dict.get(key)
