from typing import Any
from networking import NetworkMember


class RaftState:
    def __init__(self):
        self._leader_id: Any = None
        self._voted_for: NetworkMember = None
        self._current_term: int = 0
    
    @property
    def voted_for(self) -> NetworkMember | None:
        return self._voted_for
    
    @voted_for.setter
    def voted_for(self, value: NetworkMember):
        self._voted_for = value

    @property
    def current_term(self) -> int:
        return self._current_term
    
    @current_term.setter
    def current_term(self, value: int):
        self._current_term = value

    @property
    def leader_id(self) -> int:
        return self._leader_id
    
    @leader_id.setter
    def leader_id(self, value: Any):
        self.leader_id = value
