from pydantic import BaseModel, ValidationError

import json

from.raft_models import RaftLogItem
from .networking import NetworkMember


class _RaftStateModel(BaseModel):
    voted_for: str = None
    current_term: int = 0
    log: list[RaftLogItem] = []


class RaftState:
    def __init__(self, this: NetworkMember):
        self._this = this
        self._model = _RaftStateModel()
        self._commit_index: int = 0
        self._last_applied: int = 0
        self._load()


    @property
    def voted_for(self) -> str | None:
        self._load()
        return self._model.voted_for

    @voted_for.setter
    def voted_for(self, value: str):
        self._model.voted_for = value
        self._dump()


    @property
    def current_term(self) -> int:
        self._load()
        return self._model.current_term

    @current_term.setter
    def current_term(self, value: int):
        self._model.current_term = value
        self._dump()

    
    @property
    def log(self) -> list[RaftLogItem]:
        self._load()
        return self._model.log

    @log.setter
    def log(self, value: list[RaftLogItem]):
        self._model.log = value
        self._dump()

    def log_append(self, value: RaftLogItem):
        self._model.log.append(value)
        self._dump()


    @property
    def commit_index(self) -> int:
        return self._commit_index

    @commit_index.setter
    def commit_index(self, value: int):
        self._commit_index = value
    

    @property
    def last_applied(self) -> int:
        return self._last_applied

    @last_applied.setter
    def last_applied(self, value: int):
        self._last_applied = value

    
    def _load(self):
        try:
            with open(f"{self._this.id}.json", "tr") as f:
                state = _RaftStateModel.model_validate_json(f.read())
                self._model = state
        except (FileNotFoundError, ValidationError):
            pass

    
    def _dump(self):
        with open(f"{self._this.id}.json", "tw") as f:
            data = self._model.model_dump_json()
            f.write(data)
