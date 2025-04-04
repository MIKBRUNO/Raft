from typing import Any
from networking import NetworkMember
import json


class RaftState:
    def __init__(self, this: NetworkMember):
        self._this = this
        self._leader_id: Any = None
        self._voted_for: str = None
        self._current_term: int = 0
        self._load()


    @property
    def voted_for(self) -> str | None:
        self._load()
        return self._voted_for


    @voted_for.setter
    def voted_for(self, value: str):
        self._voted_for = value
        self._dump()


    @property
    def current_term(self) -> int:
        self._load()
        return self._current_term


    @current_term.setter
    def current_term(self, value: int):
        self._current_term = value
        self._dump()


    @property
    def leader_id(self) -> int:
        return self._leader_id


    @leader_id.setter
    def leader_id(self, value: Any):
        self.leader_id = value

    
    def _load(self):
        try:
            with open(f"{self._this.id}.json", "tr") as f:
                state: dict = json.load(f)
            self._current_term = state.get("currentTerm", self._current_term)
            self._voted_for = state.get("votedFor", self._voted_for)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    
    def _dump(self):
        with open(f"{self._this.id}.json", "tw") as f:
            json.dump({"currentTerm": self._current_term, "votedFor": self._voted_for}, f)
